#!/usr/bin/env python3
# Copyright (c) YugaByte, Inc.

import argparse
import glob
import logging
import os
import re
import traceback
import kubernetes
from utils import get_repo_commit_hashes_and_yugabyte_release, \
    BASE_YB_RELEASE_DIR

from release_context import LocalReleaseContext, download_os_release, install_centos_release
from ssh_utils import copy_release_to_k8s_yw
from yw_proxy import set_super_admin_user, YugaWareProxy


def validateArgs(args):
    if args.packages_location:
        if not args.is_k8s and not args.packages_location.startswith(BASE_YB_RELEASE_DIR):
            raise RuntimeError("The packages_location has to be a sub directory in {}."
                               .format(BASE_YB_RELEASE_DIR))
    else:
        if args.build_number or args.custom_package:
            args.packages_location = None
        else:
            args.packages_location = \
                max(glob.iglob(BASE_YB_RELEASE_DIR + '*'), key=os.path.getctime)


def main():
    parser = argparse.ArgumentParser()

    # Arguments if a new universe needs to be created.
    parser.add_argument('--host', required=False,
                        help='Yugaware host needed when creating a universe.')
    parser.add_argument('--port', required=False,
                        help='Port in which Yugware is listening on the `host`.')
    parser.add_argument('--keep_created_universe', action="store_true",
                        help='If present, we do not delete a universe created in this call.')
    parser.add_argument('--replication_factor', default=3,
                        help='Replication factor for universe creation.')
    parser.add_argument('--num_nodes',
                        help='Number of nodes. Defaults to replication factor.')
    parser.add_argument('--packages_location',
                        help='Directory of the yugabyte tarball. '
                             'Used for pre-existing Docker image. '
                             'Has to be full path to a child dir under /opt/yugabyte/releases.')
    parser.add_argument('--release_version', default=None,
                        help='Release Version to run itest against.')
    parser.add_argument('--build_number', type=int, default=None,
                        help="Custom build_number to specify when running locally")
    parser.add_argument('--custom_package',
                        help='Filepath to yugabyte tarball for custom YB testing. '
                             'Creates corresponding Docker image. Overrides packages_location.')

    # Argument when using an existing universe as perf run target cluster.
    parser.add_argument('--workload_servers',
                        help='Comma separated tserver host_ips. Script picks correct ports for '
                             'Redis/YQL. Will assume YW is not needed.')
    parser.add_argument('--private_key',
                        help='The private access key to ssh access nodes in workload servers.')

    # Kubernetes related flags.
    parser.add_argument('--is_k8s', action="store_true",
                        help='Determine if we are running in k8s or not')
    parser.add_argument('--k8s_cluster_name', required=False, type=str,
                        help='Name of the GKE k8s cluster to target')
    parser.add_argument('--k8s_cluster_region', default=kubernetes.K8S_CLUSTER_ITEST_ZONE,
                        type=str, help='The region where k8s cluster is located', required=False)
    parser.add_argument('--k8s_yw_namespace', required=False, type=str,
                        help='Name of the k8s namespace for YW')
    parser.add_argument('--run_load_in_k8s', action="store_true",
                        help='Flag to switch load for k8s through LB vs inside the cluster')
    args = parser.parse_args()

    has_errors = False
    using_specific_release = args.build_number is not None
    context = LocalReleaseContext()
    try:
        validateArgs(args)
        yugabyte_version = None
        repo_commit_hashes = {}

        if args.custom_package:
            repo_commit_hashes, yb_release = get_repo_commit_hashes_and_yugabyte_release(
                args.custom_package)
            context.yb_release = yb_release
            context.yugabyte_version = yb_release.version
            yugabyte_version = context.yb_release.get_release_name()
            context.build_number = yb_release.build_number or yb_release.commit
            context.packages_location = args.custom_package
            context.yugabyte_db_local = install_centos_release(
                os.path.dirname(args.custom_package),
                args.custom_package,
                context.yugabyte_version)

        elif using_specific_release:
            context.yugabyte_version = args.release_version
            context.build_number = args.build_number
            context.set_yb_release(context.build_number)
            yugabyte_version = context.yb_release.get_release_name()
            (context.packages_location,
             context.yugabyte_db_local) = download_os_release(args.packages_location,
                                                              context.yugabyte_version,
                                                              context.build_number)
        else:
            context.packages_location = args.packages_location

        if using_specific_release or args.packages_location:
            if len(list(filter(re.compile('yugabyte').match,
                               os.listdir(context.packages_location)))) == 0:
                raise RuntimeError("Did not find a yugabyte tarball in {}."
                                   .format(context.packages_location))

        # Input packages can be a directory like /opt/yugabyte/releases/1.2.0.0 or a tar file:
        # Users/yugabyte/Downloads/yugabyte-ee-1.2.0.0-064d36...3-release-centos-x86_64.tar.gz
        input_packages = args.custom_package or context.packages_location

        logging.info("Using yugabyte package in {}, instance type {}, and RF={}.".format(
            input_packages, args.instance_type, args.replication_factor))

        perf_info = {}
        if args.k8s_cluster_name is not None:
            kubernetes.get_credentials(args.k8s_cluster_name, args.k8s_cluster_region)
        kubernetes.prepare_args_and_yw(
            args, yugabyte_version,
            using_latest=(not using_specific_release and not args.custom_package),
            need_ip_alias=not args.run_load_in_k8s, is_perf_test=True)
        if args.custom_package:
            copy_release_to_k8s_yw(
                yugabyte_version, args.custom_package, args.k8s_yw_namespace)
        try:
            config = vars(args)
            config["yugabyte_version"] = yugabyte_version
            config["requires_yw"] = not args.workload_servers
            config["yugabyte_db_local"] = context.yugabyte_db_local
            if config["requires_yw"]:
                # Create super admin user for YW to allow multi-customer registration.
                try:
                    super_admin_user = set_super_admin_user(args.host)
                    yba_obj = YugaWareProxy(args.host, args.port, login_user=super_admin_user)
                    yba_obj.login(False)
                    yba_obj.get_or_create_provider()
                    logging.info("Super admin logged in.")
                except Exception as e:
                    raise RuntimeError("Could not create super admin", repr(e))

            config["is_dev"] = True
            log_summary, perf_info = run_perf_test_cases(
                config, args.host, args.port, args.perf_test_provider)
        except Exception as error:
            has_errors = True

        if not args.workload_servers:
            logging.info("All package location {}".format(input_packages))
    except Exception as error:
        has_errors = True
        logging.error("Exception : {}".format(str(error)))
        traceback.print_exc()


if __name__ == "__main__":
    main()
