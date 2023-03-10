# Copyright (c) YugaByte, Inc.

import atexit
import logging
import os
import random
import subprocess
import time

from utils import CUSTOM_GCE_NETWORK, run_devops_command, run_devops_command_fail_ok, \
    get_user_name, USER_ITEST_REPO, USER_HOME_CHARTS_REPO, TIMESTAMP, \
    DOCKER_UBI_BASE_IMAGE, ITEST_USER_ENV_VAR, get_instance_tags, GCP_SVC_ACCOUNT, \
    PTEST_K8S_CIDRS, CERT_MANAGER_YAML, k8s_cluster_name, k8s_cluster_region

YB_HELM_DIR = os.path.join(USER_HOME_CHARTS_REPO, "stable/yugabyte")
YW_HELM_DIR = os.path.join(USER_HOME_CHARTS_REPO, "stable/yugaware")
K8S_CLUSTER_ITEST_REGION = "us-west1"
K8S_CLUSTER_ITEST_ZONE = "us-west1-b"
K8S_CLUSTER_ITEST_MACHINE_TYPE = "n1-standard-32"
K8S_LOADTESTER_CPU = 31

K8S_CLUSTER_ITEST_CIDR = "0.0.0.0/16"

K8S_NODEPOOL_NAME_PREFIX = "my-pool"
K8S_NODEPOOL_MACHINE_TYPE = "n1-standard-16"
K8S_NODEPOOL_NUM_NODES = "10"

# These variables must be present in the environment.
SECRETS_ENV_VAR = "SECRETS_PATH"
# For old release pass quay secret in case of dev-itest pipeline
K8S_ITEST_QUAY_SECRETS_ENV_VAR = None
# Pick QUAY pull secret explicitly for dev-itest pipeline
if "ci" in os.environ.get(ITEST_USER_ENV_VAR, ""):
    K8S_ITEST_QUAY_SECRETS_ENV_VAR = "K8S_ITEST_QUAY_SECRETS_PATH"
K8S_ITEST_CONFIG_PATH_ENV_VAR = "K8S_ITEST_CONFIG_PATH"
K8S_PULL_SECRET_ENV_VAR = "K8S_PULL_SECRET_PATH"

K8S_KUBECONFIG_TMP_FILE = "/tmp/yugabyte-helm.conf"
K8S_YUGAWARE_STORAGE_CLASS = "yb-standard"

# This is hardcoded to match the loadtester.yaml.
LOADTEST_DEPLOYMENT_NAME = "yb-loadtester"
LOADTESTER_DEPLOYMENT_YML_PATH = os.path.join(USER_ITEST_REPO, "loadtester.yaml")

K8S_TEST_STORAGE_YAML = os.path.join(USER_ITEST_REPO, "src/custom_storage.yaml")

CPU_RESOURCE_REQ = {
    'yugaware': 5,
    'nginx': 0.25,
    'postgres': 0.5,
    'prometheus': 0.5
}
MEM_RESOURCE_REQ = {
    'yugaware': '8Gi',
    'nginx': '300Mi',
    'postgres': '1Gi',
    'prometheus': '4Gi'
}


def get_yw_image_registry(is_ubi=False):
    if is_ubi:
        yw_image_registry = "quay.io/yugabyte/yugaware"
    else:
        yw_image_registry = "quay.io/yugabyte/yugaware"
    env_container_registry = os.environ.get("CONTAINER_REGISTRY")
    if env_container_registry:
        repo_name = "YW_UBI_IMAGE_REPO" if is_ubi else "YW_IMAGE_REPO"
        env_yw_image_repo = os.environ.get(repo_name)
        if not env_yw_image_repo:
            raise RuntimeError("Image repo env var {} not defined".format(repo_name))
        yw_image_registry = env_container_registry + "/" + env_yw_image_repo
    return yw_image_registry


def get_yb_image_registry(is_ubi=False):
    if is_ubi:
        yb_image_registry = "quay.io/yugabyte/yugabyte-ubi-itest"
    else:
        yb_image_registry = "quay.io/yugabyte/yugabyte-itest"
    env_container_registry = os.environ.get("CONTAINER_REGISTRY")
    if env_container_registry:
        repo_name = "YB_UBI_IMAGE_REPO" if is_ubi else "YB_IMAGE_REPO"
        env_yb_image_repo = os.environ.get(repo_name)
        if not env_yb_image_repo:
            raise RuntimeError("Image repo env var {} not defined".format(repo_name))
        yb_image_registry = env_container_registry + "/" + env_yb_image_repo
    return yb_image_registry


def get_sample_apps_image_registry():
    sa_image_registry = "quay.io/yugabyte/yb-sample-apps-itest"
    env_container_registry = os.environ.get("CONTAINER_REGISTRY")
    env_sa_image_repo = os.environ.get("SAMPLE_APPS_IMAGE_REPO")
    if env_container_registry and env_sa_image_repo:
        sa_image_registry = env_container_registry + "/" + env_sa_image_repo
    return sa_image_registry


def get_image_repos(is_ubi=False):
    return {
        "yugaware": get_yw_image_registry(is_ubi).rsplit('/', 1)[1],
        "yugabyte": get_yb_image_registry(is_ubi).rsplit('/', 1)[1],
        "yb-sample-apps": get_sample_apps_image_registry().rsplit('/', 1)[1]
    }


def _call_and_log(cmd_list, namespace=None):
    logging.info("Running: %s", cmd_list)
    try:
        output = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT).decode('utf-8')
        logging.debug("Output: %s", output)
        return output
    except subprocess.CalledProcessError as e:
        logging.error("Command '{}' failed with returncode: {} and output:\n{}".format(
            cmd_list, e.returncode, e.output))
        if namespace:
            describe_all(namespace)
        raise e


###################################################################################################
# CLUSTER
###################################################################################################
def get_credentials(cluster_name, cluster_region):
    logging.info("Fetching credentials for cluster: {}".format(cluster_name))
    cmd = [
        "gcloud", "container", "clusters", "get-credentials", "--region", cluster_region,
        cluster_name
    ]
    _call_and_log(cmd)


def create_nodepool(cluster_name, cluster_region, suffix, num_nodes):
    nodepool_name = "{}-{}".format(K8S_NODEPOOL_NAME_PREFIX, suffix)
    logging.info("Creating a new node pool for cluster: {}".format(cluster_name))
    cmd = [
        "gcloud", "container", "node-pools", "create", nodepool_name, "--cluster",
        cluster_name, "--region", cluster_region, "--machine-type",
        K8S_NODEPOOL_MACHINE_TYPE, "--num-nodes", str(num_nodes)
    ]
    _call_and_log(cmd)


def create_cluster(cluster_name, cluster_region, need_ip_alias, is_perf_test=False):
    logging.info("Creating k8s cluster: {}".format(cluster_name))
    labels_str = "--labels="
    for tag in get_instance_tags(is_perf_test):
        labels_str += "{}={},".format(tag['name'], tag['value'])

    labels_str = labels_str.rstrip(",")

    # Create the cluster.
    cmd = [
        "gcloud", "container", "clusters", "create", cluster_name, "--region", cluster_region,
        "--machine-type", K8S_CLUSTER_ITEST_MACHINE_TYPE, "--network", CUSTOM_GCE_NETWORK,
        "--subnetwork", "subnet-{}".format(K8S_CLUSTER_ITEST_REGION),
        "--service-account", GCP_SVC_ACCOUNT, labels_str,
        "--cluster-ipv4-cidr", random.choice(PTEST_K8S_CIDRS),
        "--enable-autoscaling", "--min-nodes=3", "--max-nodes=8"
    ]
    if need_ip_alias:
        cmd.append("--enable-ip-alias")
    _call_and_log(cmd)


def apply_secret(namespace):
    # In case of old releases apply QUAY secret if available
    user = os.environ.get(ITEST_USER_ENV_VAR)
    if K8S_ITEST_QUAY_SECRETS_ENV_VAR:
        cmd_quay_secret = [
            "kubectl", "apply", "-f", os.environ[K8S_ITEST_QUAY_SECRETS_ENV_VAR], "-n", namespace
        ]
        _call_and_log(cmd_quay_secret)

    cmd = [
        "kubectl", "apply", "-f", os.environ[SECRETS_ENV_VAR], "-n", namespace
    ]
    _call_and_log(cmd)


def is_helm2():
    return _call_and_log(["helm", "version"]).find('v3.') < 0


def apply_yamls(namespace):
    # Apply the rbac.
    rbac_yml_path = os.path.join(YB_HELM_DIR, "yugabyte-rbac.yaml")
    cmd = [
        "kubectl", "apply", "-f", rbac_yml_path
    ]
    _call_and_log(cmd)

    # Create the relevant namespace.
    cmd = [
        "kubectl", "create", "ns", namespace
    ]
    _call_and_log(cmd)

    apply_secret(namespace)

    # Apply the storage class yaml.
    cmd = [
        "kubectl", "apply", "-f", K8S_TEST_STORAGE_YAML
    ]
    _call_and_log(cmd)

    if is_helm2():
        # Make sure helm is initialized.
        cmd = [
            "helm", "init", "--upgrade", "--wait", "--service-account", "yugabyte-helm"
        ]
        try:
            _call_and_log(cmd)
        except subprocess.CalledProcessError as e:
            if "current Tiller version" in str(e.output):
                logging.info("Ignoring helm version mismatch errors")
            else:
                raise e

    # Generate the kubeconfig file if its not present.
    if not os.path.isfile(K8S_KUBECONFIG_TMP_FILE):
        cmd = [
            "python", os.path.join(YB_HELM_DIR, "generate_kubeconfig.py"), "-s", "yugabyte-helm"
        ]
        _call_and_log(cmd)


def delete_nodepool(cluster_name, cluster_region, suffix):
    nodepool_name = "{}-{}".format(K8S_NODEPOOL_NAME_PREFIX, suffix)
    logging.info("Deleting node pool created in cluster: {}".format(cluster_name))
    cmd = [
        "gcloud", "-q", "container", "node-pools", "delete", nodepool_name, "--cluster",
        cluster_name, "--region", cluster_region
    ]
    _call_and_log(cmd)


def delete_cluster(cluster_name, cluster_region):
    logging.info("Deleting cluster {}".format(cluster_name))
    # Note: need -q to prevent prompt on delete.
    cmd = [
        "gcloud", "-q", "container", "clusters", "delete", cluster_name, "--region",
        cluster_region
    ]
    _call_and_log(cmd)


###################################################################################################
# YUGAWARE
###################################################################################################
def install_yugaware(release_version, namespace, chart=YW_HELM_DIR, custom_chart_registry=None,
                     security_privilege="non-root"):
    logging.info("Installing YW version: {}".format(release_version))
    cpu_req_values = ",".join([
        "{}.resources.requests.cpu={}".format(container, req)
        for (container, req) in CPU_RESOURCE_REQ.items()])
    mem_req_values = ",".join([
        "{}.resources.requests.memory={}".format(container, req)
        for (container, req) in MEM_RESOURCE_REQ.items()])
    # To avoid Yugaware accessible in public internet making Load Balancer Internal
    internal_load_balancer = "yugaware."\
        "service.annotations.networking\\.gke\\.io\\/load-balancer-type=Internal"
    # YW Image Registry
    yw_image_registry = custom_chart_registry if custom_chart_registry else get_yw_image_registry()

    if is_helm2():
        cmd = [
            "helm", "install", chart, "--name", namespace, "--namespace", namespace,
            "--wait", "--timeout", "1200"
        ]
    else:
        cmd = [
            "helm", "install", namespace, chart, "--namespace", namespace,
            "--wait", "--timeout", "1200s"
        ]

    # Set the image pull policy to Always to ensure we always have the latest version of
    # the requested image tag on the cluster.
    set_policy = "--set=yugaware.multiTenant=true,{},{},image.repository={},image.tag={}," \
                 "yugaware.storageClass={},"\
                 "image.pullPolicy=Always,{}".format(cpu_req_values,
                                                     mem_req_values,
                                                     yw_image_registry,
                                                     release_version,
                                                     K8S_YUGAWARE_STORAGE_CLASS,
                                                     internal_load_balancer)

    # set universe creation/update helm timeout to 3600s to avoid GKE load balancer issues.
    helm_timeout = "--set=helm.timeout=3600"
    if not custom_chart_registry and K8S_ITEST_QUAY_SECRETS_ENV_VAR:
        pull_secret_file = "gcr-pull-secret"
        set_policy = set_policy + ",image.pullSecret={}".format(pull_secret_file)

    # # Non-root security context in case of version >= 2.17.2.0
    # if is_release_current(release_version, "2.17.2.0-b1") and security_privilege == "non-root":
    #     non_root_policies = ",securityContext.enabled=true" \
    #                         ",securityContext.runAsUser=10002" \
    #                         ",securityContext.runAsGroup=10002" \
    #                         ",securityContext.fsGroup=10002"
    #     set_policy = set_policy + non_root_policies

    cmd.append(set_policy)
    cmd.append(helm_timeout)

    _call_and_log(cmd, namespace)
    # Sometimes wait doesn't actually wait until YW is up and running...
    wait_for_yugaware(namespace)
    return get_yw_lb_ip(namespace)


def upgrade_yugaware(target_version, namespace, chart=YW_HELM_DIR, custom_chart_registry=None):
    logging.info("Upgrading YW version to {}".format(target_version))
    cpu_req_values = ",".join([
        "{}.resources.requests.cpu={}".format(container, req)
        for (container, req) in CPU_RESOURCE_REQ.items()])
    mem_req_values = ",".join([
        "{}.resources.requests.memory={}".format(container, req)
        for (container, req) in MEM_RESOURCE_REQ.items()])
    # To avoid Yugaware accessible in public internet making Load Balancer Internal
    internal_load_balancer = "yugaware."\
        "service.annotations.networking\\.gke\\.io\\/load-balancer-type=Internal"

    # Pull secret and YW Image Registry
    if custom_chart_registry and K8S_ITEST_QUAY_SECRETS_ENV_VAR:
        pull_secret_file = "gcr-pull-secret"
        yw_image_registry = custom_chart_registry
    else:
        pull_secret_file = "yugabyte-k8s-pull-secret"
        yw_image_registry = get_yw_image_registry()

    if is_helm2():
        cmd = [
            "helm", "upgrade", chart, "--name", namespace, "--namespace", namespace,
            "--wait", "--timeout", "1200"
        ]
    else:
        cmd = [
            "helm", "upgrade", namespace, chart, "--namespace", namespace,
            "--wait", "--timeout", "1200s"
        ]

    # Set the image pull policy to Always to ensure we always have the latest version of
    # the requested image tag on the cluster.
    set_policy = "--set=yugaware.multiTenant=true,{},{},image.repository={}," \
                 "image.tag={},yugaware.storageClass={}," \
                 "image.pullPolicy=Always,{},image.pullSecret={}".format(cpu_req_values,
                                                                         mem_req_values,
                                                                         yw_image_registry,
                                                                         target_version,
                                                                         K8S_YUGAWARE_STORAGE_CLASS,
                                                                         internal_load_balancer,
                                                                         pull_secret_file)
    cmd.append(set_policy)

    _call_and_log(cmd)
    # Sometimes wait doesn't actually wait until YW is up and running...
    wait_for_yugaware(namespace)
    return get_yw_lb_ip(namespace)


def upgrade_yugaware_security_privilege(yugabyte_version, namespace, chart=YW_HELM_DIR,
                                        custom_chart_registry=None, security_privilege=None):
    logging.info("Upgrading YW security privilege {}".format(security_privilege))
    # To avoid Yugaware accessible in public internet making Load Balancer Internal
    internal_load_balancer = "yugaware."\
        "service.annotations.networking\\.gke\\.io\\/load-balancer-type=Internal"

    # Pull secret and YW Image Registry
    if custom_chart_registry and K8S_ITEST_QUAY_SECRETS_ENV_VAR:
        pull_secret_file = "gcr-pull-secret"
        yw_image_registry = custom_chart_registry
    else:
        pull_secret_file = "yugabyte-k8s-pull-secret"
        yw_image_registry = get_yw_image_registry()

    cmd = [
        "helm", "upgrade", namespace, chart, "--namespace", namespace,
        "--wait", "--timeout", "1200s"
    ]

    # Set the image pull policy to Always to ensure we always have the latest version of
    # the requested image tag on the cluster.
    set_policy = "--set=yugaware.multiTenant=true,image.repository={}," \
                 "image.tag={},yugaware.storageClass={}," \
                 "image.pullPolicy=Always,{},image.pullSecret={}".format(yw_image_registry,
                                                                         yugabyte_version,
                                                                         K8S_YUGAWARE_STORAGE_CLASS,
                                                                         internal_load_balancer,
                                                                         pull_secret_file)

    # Non-root security context in case of version >= 2.17.2.0
    if is_release_current(yugabyte_version, "2.17.2.0-b1"):
        upgrade_security_policies = None
        if security_privilege == "non-root":
            upgrade_security_policies = ",securityContext.enabled=true" \
                                        ",securityContext.runAsUser=10002" \
                                        ",securityContext.runAsGroup=10002" \
                                        ",securityContext.fsGroup=10002"
        elif security_privilege == "root":
            upgrade_security_policies = ",securityContext.enabled=false"
        set_policy = set_policy + upgrade_security_policies

    cmd.append(set_policy)

    _call_and_log(cmd)
    # Sometimes wait doesn't actually wait until YW is up and running...
    wait_for_yugaware(namespace)
    return get_yw_lb_ip(namespace)


def get_yw_pod(yw_namespace):
    return "{}-yugaware-0".format(yw_namespace)


def delete_yugaware(name):
    logging.info("Removing YW release: {}".format(name))
    if is_helm2():
        cmd = [
            "helm", "del", "--purge", name
        ]
    else:
        cmd = [
            "helm", "delete", "--namespace", name, name
        ]

    _call_and_log(cmd)
    if os.path.isfile(K8S_KUBECONFIG_TMP_FILE):
        logging.info("Removing local conf file: {}".format(K8S_KUBECONFIG_TMP_FILE))
        os.remove(K8S_KUBECONFIG_TMP_FILE)
    logging.info("Removing namespace: {}".format(name))
    cmd = [
        "kubectl", "delete", "namespace", name
    ]
    _call_and_log(cmd)


def get_yw_lb_ip(namespace):
    cmd = [
        "kubectl", "get", "svc", "{}-yugaware-ui".format(namespace), "-n", namespace,
        "-o", "jsonpath={.status.loadBalancer.ingress[0].ip}"
    ]
    return _call_and_log(cmd).strip()


def wait_for_resource(namespace, resource_name, cmd, expected_output):
    num_iters = 0
    # Wait for 2 minutes.
    max_iters = 2 * 12
    sleep_interval = 5
    while num_iters <= max_iters:
        try:
            num_iters += 1
            output = _call_and_log(cmd)
            logging.info(output)
            if expected_output in output:
                return
            time.sleep(sleep_interval)
        except Exception as e:
            time.sleep(sleep_interval)
    raise RuntimeError("Could not find resource {}".format(resource_name))


def wait_for_deployment(namespace, deployment_name):
    cmd = [
        "kubectl", "rollout", "status", "-n", namespace,
        "deployment.v1.apps/{}".format(deployment_name)
    ]
    wait_for_resource(namespace, deployment_name, cmd, "successfully rolled out")


def wait_for_pod(namespace, pod_name):
    cmd = [
        "kubectl", "get", "pods", "-n", namespace, pod_name,
        "-o", "jsonpath={.status.phase}"
    ]
    wait_for_resource(namespace, pod_name, cmd, "Running")


def wait_for_yugaware(namespace):
    wait_for_pod(namespace, get_yw_pod(namespace))


def describe_all(namespace):
    cmd = [
        "kubectl", "describe", "all", "-n", namespace
    ]
    _call_and_log(cmd)


###################################################################################################
# LOADTESTER
###################################################################################################
def create_loadtester(namespace):
    logging.info("Creating a load tester in namespace {}".format(namespace))

    # Apply the secret so we can download the image from quay.
    apply_secret(namespace)

    cmd = [
        "kubectl", "create", "-n", namespace, "-f", LOADTESTER_DEPLOYMENT_YML_PATH
    ]
    _call_and_log(cmd)
    wait_for_deployment(namespace, LOADTEST_DEPLOYMENT_NAME)

    pod_name = get_loadtester_name(namespace)
    wait_for_pod(namespace, pod_name)


def delete_loadtester(namespace):
    logging.info("Deleting a load tester in namespace {}".format(namespace))
    cmd = [
        "kubectl", "delete", "deployment", LOADTEST_DEPLOYMENT_NAME, "-n", namespace
    ]
    _call_and_log(cmd)


def get_loadtester_name(namespace):
    logging.info("Fetching load tester name.")
    cmd = [
        "kubectl", "get", "pod", "-n", namespace, "-l", "app={}".format(LOADTEST_DEPLOYMENT_NAME),
        "-o", "jsonpath={.items[0].metadata.name}"
    ]
    return _call_and_log(cmd).strip()


###################################################################################################
# RELEASE
###################################################################################################
def release_internal_repo(release, code_repo, release_path, flag_latest=False, is_ubi=False):
    tag = release.get_release_name()
    remote_repo = get_image_repos(is_ubi)[code_repo]
    logging.info("Preparing {} release for tag: {}".format(code_repo, tag))
    cmd = [
        "bin/ybcloud.sh", "docker", "image", "create", "--local_releases", release_path,
        "--package_name", code_repo, "--remote_package_name", remote_repo,
        "--tag", tag, "--publish"
    ]

    if flag_latest:
        cmd.extend(["--remote_package_tag", "latest", "--remote_package_tag", tag])

    if is_ubi:
        cmd.extend(["--base_image", DOCKER_UBI_BASE_IMAGE])
        run_devops_command_fail_ok(cmd)
    else:
        run_devops_command(cmd)


def prepare_args_and_yw(args, yugabyte_version, using_latest, need_ip_alias=False,
                        is_perf_test=False):
    """
    Prepare args and cluster to run YW depending on cmdline args
    """
    wait_for_yw = False
    keep_resources = getattr(args, "keep_created_universe", False)
    # If we were not provided a custom host, then amend args with the appropriate host.
    if not args.host:
        if args.is_k8s:
            suffix = "{}-{}".format(get_user_name(), TIMESTAMP)
            logging.info("k8s is set to true")
            if not args.k8s_cluster_name:
                # Create the cluster
                args.k8s_cluster_name = "yb-itest-{}".format(suffix)
                if not keep_resources:
                    atexit.register(lambda: delete_cluster(
                        args.k8s_cluster_name, args.k8s_cluster_region))
                create_cluster(args.k8s_cluster_name, args.k8s_cluster_region,
                               need_ip_alias, is_perf_test)
            # Setup a NS for YW if one is not provided.
            if not args.k8s_yw_namespace:
                args.k8s_yw_namespace = "yw-ns-{}".format(suffix)
            apply_yamls(args.k8s_yw_namespace)
            # Now we either just created a cluster or fetched the credentials for one. Time to
            # install YW.
            yw_tag = "latest" if using_latest else yugabyte_version
            if not keep_resources:
                atexit.register(
                    lambda: delete_yugaware(args.k8s_yw_namespace))
            yw_ip = install_yugaware(yw_tag, args.k8s_yw_namespace)
            args.host = yw_ip
            args.port = 80
            wait_for_yw = True
        else:
            # If not in k8s, just default to localhost.
            args.host = "localhost"
            # Do not wait for YW to restart if using latest deploy, since that means YW is up.
            wait_for_yw = not using_latest
    else:
        if args.is_k8s:
            if not args.k8s_cluster_name:
                raise RuntimeError(
                    "We require a k8s_cluster_name in order to get the credentials.")
            if not args.k8s_yw_namespace:
                raise RuntimeError(
                    "We require a k8s_yw_namespace in order to access the YW service.")
            get_credentials(args.k8s_cluster_name, args.k8s_cluster_region)
    # Default port as well based on k8s or not.
    if not args.port:
        args.port = 80 if args.is_k8s else 9000

    # Give a minute for yugaware service to restart.
    if wait_for_yw:
        time.sleep(60)


def setup_cert_manager():
    # Check if cert-manager namespace is installed
    cmd = ["kubectl", "get", "namespace", "cert-manager"]
    logging.info("Running: %s", cmd)
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf-8')
        logging.debug("Output: %s", output)
    except subprocess.CalledProcessError as e:
        # Create the namespace if not present
        _call_and_log(
            ["kubectl", "apply", "-f", CERT_MANAGER_YAML]
        )

    # Apply the cert_manager_yaml.
    cert_manager_yml_path = os.path.join(USER_ITEST_REPO, "src/cert_manager.yaml")
    cmd = [
        "kubectl", "apply", "-f", cert_manager_yml_path
    ]
    _call_and_log(cmd)
