# Copyright (c) YugaByte, Inc.

import copy
import json
import logging
import os
import random

import kubernetes
from release import is_release_current
from utils import CUSTOM_GCE_NETWORK, CUSTOM_AWS_VPC, GCP_LOAD_TESTER_REGION, TIMESTAMP, \
    GCP_REGIONS, CUSTOM_AZU_VNET, CUSTOM_HOME_DIR_CONFIG, GCP_LOAD_TESTER_AZ, AuthSettings, \
    ITEST_USER_ENV_VAR, OS_USER_AMI, K8S_PROVIDER_ANNOTATIONS, CERT_MANAGER_METADATA
from universe_tests.backup import BackupStorageType


class UniverseDetails:
    def __init__(self, universe_json):
        self.universe_name = universe_json['name']
        self.universe_uuid = universe_json['universeUUID']
        self.current_nodes = universe_json['universeDetails']['nodeDetailsSet']
        self.node_names = [node['nodeName'] for node in self.current_nodes]
        self.rootCA = universe_json['universeDetails'].get('rootCA')
        # Cloud Config
        self.clientRootCA = universe_json['universeDetails'].get('clientRootCA')
        self.kms_config = universe_json['universeDetails']['encryptionAtRestConfig']

        # Primary cluster.
        self.user_intent = None
        self.placement_info = None
        self.cluster_uuid = None
        self.regions = universe_json['universeDetails']['clusters'][0]['regions']
        for cluster_json in universe_json['universeDetails']['clusters']:
            if cluster_json['clusterType'] == 'PRIMARY':
                self.user_intent = cluster_json['userIntent']
                self.placement_info = cluster_json['placementInfo']
                self.cluster_uuid = cluster_json['uuid']
                break
        self.assignStaticPublicIP = self.user_intent.get('assignStaticPublicIP')
        logging.info("StaticPublicIP:={}".format(cluster_json.get('self.assignStaticPublicIP')))
        # Async cluster(s).
        # TODO: do more than 1 in the future?
        self.async_user_intent = None
        self.async_placement_info = None
        self.async_cluster_uuid = None
        # Better way to check according to version?
        if universe_json['universeDetails'].get('communicationPorts'):
            self.communication_ports = universe_json['universeDetails'].get('communicationPorts')
        for cluster_json in universe_json['universeDetails']['clusters']:
            if cluster_json['clusterType'] == 'ASYNC':
                if self.async_user_intent is not None:
                    raise RuntimeError("Currently do not support multiple async clusters...")
                self.async_user_intent = cluster_json['userIntent']
                self.async_placement_info = cluster_json['placementInfo']
                self.async_cluster_uuid = cluster_json['uuid']
        # Metadata storing YB package s3 path if needed.
        self.s3_package_path = ""

        # TODO: do we need this? why can't we just use the YW returned UserIntent?
        self.custom_user_intent = self.user_intent


class CloudConfig(object):
    def __init__(self):
        self.use_default_yw = True
        self.cloud_provider_code = 'default'
        self.cloud_provider_region = None
        self.async_region = None
        self.custom_vpc_id = None
        self.custom_host_vpc_id = None
        self.custom_ssh_user = "centos"
        self.os = "Centos7"
        self.custom_image_id = None
        self.custom_az_mapping = None
        self.custom_sg_id = None
        self.custom_ssh = False
        self.provider_name = None
        self.instance_type = None
        self.upgraded_instance_type = None
        self.upgraded_volume_size = None
        self.device_info = None
        self.same_rootCA = True
        # updating default value for UseTimeSync to True, as currenlty in
        # UI UseTimeSync is True by default
        self.use_time_sync = True
        # Following two flags are for node to node TLS and client to node TLS
        # we should enable enable_nton_tls whenever we enable enable_cton_tls
        self.enable_nton_tls = False
        self.enable_cton_tls = False
        self.enable_ysql = True
        self.enable_enc_at_rest = False
        self.enable_enc_source = False
        self.enable_enc_target = False
        self.kms_provider = "AWS"
        self.replication_factor = 3
        # Adding Number of Nodes value default to replication factor
        self.num_nodes = 3
        # TODO: remove 'extra_replication_factor' below and support 2 (or even more)
        #       CloudConfig-s in one test
        self.extra_replication_factor = 3
        self.ssh_port = 54422
        self.use_systemd = False

        self.kms_config_uuid = None
        self.provider_uuid = None
        self.access_key_code = None
        self.private_key = None
        self.db_config = {}
        # The name field will be used for identifying the test combo in logging.
        self.name = 'default'
        # Default backups to S3 so it's GCP friendly for now.
        self.backup_storage_type = BackupStorageType.S3
        # We need this when bootstrapping.
        self.provider_config = None
        # Determines if instances should download YB package from s3 or YW.
        self.should_download_from_s3 = False
        self.custom_ports = False
        # Specified as an A.B.C.D release version, will only run with this config, if the itest
        # release_version is at least this recent.
        self.minimum_version = None
        self.upgrade_base_version = None
        self.upgrade_base_build = None
        self.should_use_remote_package_path = False
        self.rehydration_image = None
        self.custom_az = None
        self.with_gflags = False
        # Custom Home Directory config specifically for onprem provider
        self.home_dir_config = None
        # Custom user feature for onprem provider
        self.onprem_custom_user = None
        # Creating Static Public IP for cloud config
        self.assign_static_public_ip = None
        self.airgap_provider = False
        self.install_node_exporter = True
        self.skip_provisioning = False
        self.auth_settings = None
        self.cloud_tier = None
        self.verify_health_check = False
        self.assign_public_ip = True
        self.secondary_subnet_config = None
        self.download_logs = False
        self.base_cloud_ami = None
        self.support_bundle = False
        self.use_cloud_ami = None
        self.cloud_architecture = "x86_64"
        self.ssh_user_privilege = "sudo"
        self.multiregion_univ_config = None  # For Multi-Region Universe config
        self.multiregion_provider_config = None  # For Multi-Region Provider config
        self.enable_geo_partition = False
        self.default_region = None
        # In case of custom build, check if avilable for respective pipeline
        self.is_acceptable_build = True
        self.is_odd_release = False
        self.run_only_build = None
        # Default to not using YBA Installer when performing Yugabundle related tests
        # (overrided in get_vm_platform() when YBA Installer should be used).
        self.use_yba_installer = False
        self.upgrade_test_path = None
        self.preferred_zone = False
        self.dedicated_nodes = False
        self.add_new_region = None
        self.remove_new_region = None
        self.validate_metrics = False

    def is_acceptable_version(self, current_version):
        logging.info("current_version:={}".format(current_version))
        logging.info("self.minimum_version:={}".format(self.minimum_version))
        return self.minimum_version is None or \
            is_release_current(current_version, self.minimum_version)

    def requires_yw(self):
        return self.use_default_yw

    def format_test_name(self, test_cls):
        return "{}-{}".format(test_cls.__name__, self.name).replace(" ", "-").lower()

    def bootstrapped(
            self,
            provider_uuid,
            access_key_code,
            private_key,
            kms_config_uuid=None,
            config={}
    ):
        self.provider_uuid = provider_uuid
        self.access_key_code = access_key_code
        self.private_key = private_key
        self.db_config = config
        self.kms_config_uuid = kms_config_uuid

    def set_unique_provider_prefix(self, test_name):
        self.provider_name = '{}-{}'.format(test_name, TIMESTAMP)

    def get_provider_config(self):
        return self.provider_config

    def is_k8s(self):
        return self.cloud_provider_code == "kubernetes"

    def _setup_gcp_config_from_env(self):
        config_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS', "")
        if os.path.isfile(config_file):
            config_content = json.loads(open(config_file, 'r').read())
            self.provider_config = {
                "config_file_contents": config_content
            }
            return True
        else:
            self.provider_config = {
                "config_file_contents": {}
            }
            return False

    def _setup_aws_config_from_env(self):
        self.provider_config = {
            'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY')
        }

    def _setup_azu_config_from_env(self):
        self.provider_config = {
            'AZURE_TENANT_ID': os.environ.get('AZURE_TENANT_ID'),
            'AZURE_SUBSCRIPTION_ID': os.environ.get('AZURE_SUBSCRIPTION_ID'),
            'AZURE_CLIENT_SECRET': os.environ.get('AZURE_CLIENT_SECRET'),
            'AZURE_CLIENT_ID': os.environ.get('AZURE_CLIENT_ID'),
            'AZURE_RG': os.environ.get('AZURE_RG')
        }

    def randomize_regions(self):
        if self.cloud_provider_code != 'gcp' or self.secondary_subnet_config is not None:
            return

        def randomize_region(region):
            possible_regions = copy.deepcopy(GCP_REGIONS)
            possible_regions.remove(region)
            return random.choice(list(possible_regions))

        if self.cloud_provider_region in GCP_REGIONS:
            self.cloud_provider_region = randomize_region(self.cloud_provider_region)
        if self.async_region:
            if self.async_region in GCP_REGIONS:
                self.async_region = self.cloud_provider_region
            else:
                self.cloud_provider_region = self.async_region

    @staticmethod
    def get_local():
        cc = CloudConfig()
        cc.use_default_yw = False
        return cc

    @staticmethod
    def get_aws_rf3(minimum_version=None, auth_enabled=False, preferred_zone=False,
                    num_volumes=1, volume_size=50):
        cc = CloudConfig()
        cc.name = "AWS RF3"
        cc.cloud_provider_code = 'aws'
        cc.cloud_provider_region = 'us-west-2'
        cc.custom_vpc_id = CUSTOM_AWS_VPC
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-6553f513",
            "us-west-2b": "subnet-f840ce9c",
            "us-west-2c": "subnet-01ac5b59",
            "us-west-2d": "subnet-00c0262486e927ff3"
        }
        cc.custom_sg_id = "sg-139dde6c"
        cc.async_region = 'us-west-2'
        if preferred_zone:
            cc.preferred_zone = "us-west-2b"
        # 2 CPU c5.
        cc.instance_type = 'c5.large'
        # 4 CPU c5.
        cc.upgraded_instance_type = 'c5.xlarge'
        cc.device_info = {'numVolumes': num_volumes, 'volumeSize': volume_size,
                          'storageType': 'GP2'}
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        if auth_enabled:
            cc.auth_settings = AuthSettings()
        cc._setup_aws_config_from_env()
        if minimum_version:
            cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_aws_rf3_c5xlarge_upgrade(base_branch_version, base_build_number, minimum_version=None,
                                     numVolumes=1, volumeSize=50):
        cc = CloudConfig()
        cc.cloud_provider_code = 'aws'
        cc.name = "AWS RF3 UPGRADE {}-b{}".format(base_branch_version, base_build_number)
        cc.upgrade_base_version = base_branch_version
        cc.upgrade_base_build = base_build_number
        cc.cloud_provider_region = 'us-west-2'
        cc.custom_vpc_id = CUSTOM_AWS_VPC
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-6553f513",
            "us-west-2b": "subnet-f840ce9c",
            "us-west-2c": "subnet-01ac5b59",
            "us-west-2d": "subnet-00c0262486e927ff3"
        }
        cc.custom_sg_id = "sg-139dde6c"
        cc.async_region = 'us-west-2'
        # 2 CPU c5.
        cc.instance_type = 'c5.xlarge'
        # 4 CPU c5.
        cc.upgraded_instance_type = 'c5.xlarge'
        cc.device_info = {'numVolumes': numVolumes, 'volumeSize': volumeSize,
                          'storageType': 'GP2'}
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        cc._setup_aws_config_from_env()
        if minimum_version:
            cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_aws_rf3_c5xlarge(minimum_version=None, numVolumes=1, volumeSize=50):
        cc = CloudConfig()
        cc.name = "AWS RF3"
        cc.cloud_provider_code = 'aws'
        cc.cloud_provider_region = 'us-west-2'
        cc.custom_vpc_id = CUSTOM_AWS_VPC
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-6553f513",
            "us-west-2b": "subnet-f840ce9c",
            "us-west-2c": "subnet-01ac5b59",
            "us-west-2d": "subnet-00c0262486e927ff3"
        }
        cc.custom_sg_id = "sg-139dde6c"
        cc.async_region = 'us-west-2'
        # 2 CPU c5.
        cc.instance_type = 'c5.xlarge'
        # 4 CPU c5.
        cc.upgraded_instance_type = 'c5.xlarge'
        cc.device_info = {'numVolumes': numVolumes, 'volumeSize': volumeSize,
                          'storageType': 'GP2'}
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        cc._setup_aws_config_from_env()
        if minimum_version:
            cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_aws_arm_rf3_m6gmedium(minimum_version=None, numVolumes=1, volumeSize=50):
        cc = CloudConfig()
        cc.name = "AWS aarch64"
        cc.custom_ssh = True
        cc.custom_ssh_user = "ec2-user"
        cc.cloud_provider_code = 'aws'
        cc.cloud_provider_region = 'us-west-2'
        cc.custom_vpc_id = CUSTOM_AWS_VPC
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-6553f513",
            "us-west-2b": "subnet-f840ce9c",
            "us-west-2c": "subnet-01ac5b59",
            "us-west-2d": "subnet-00c0262486e927ff3"
        }
        cc.custom_sg_id = "sg-139dde6c"
        cc.async_region = 'us-west-2'
        cc.device_info = {'numVolumes': numVolumes, 'volumeSize': volumeSize,
                          'storageType': 'GP2'}
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        cc.upgraded_instance_type = 'c5.xlarge'
        cc._setup_aws_config_from_env()
        if minimum_version:
            cc.minimum_version = minimum_version
        cc._setup_aws_config_from_env()
        # Alma Linux 8 https://wiki.almalinux.org/cloud/AWS.html#community-amis
        cc.custom_image_id = "ami-017b11a65709517fe"
        cc.cloud_architecture = "aarch64"
        # Changing instance to Graviton Type
        cc.instance_type = 'm6g.medium'
        return cc

    @staticmethod
    def get_aws_rf3_vm_image_upgrade():
        cc = CloudConfig.get_aws_rf3()
        cc.minimum_version = "2.8.0.0-b1"
        cc.rehydration_image = 'ami-0821373572f69d497'  # centos7-006
        cc.custom_az = cc.cloud_provider_region + 'b'
        return cc

    @staticmethod
    def get_aws_rf3_upgrade(branch_version, build_number, minimum_version=None,
                            is_odd_release=False, run_only_build=None, with_gflags=False,
                            auth_enabled=False, num_volumes=1, volume_size=50):
        cc = CloudConfig.get_aws_rf3(auth_enabled=auth_enabled,
                                     num_volumes=num_volumes,
                                     volume_size=volume_size)
        if is_odd_release:
            cc.name = "AWS RF3 ODD UPGRADE {}_{}".format(branch_version, build_number)
        elif run_only_build:
            cc.name = "AWS RF3 ODD UPGRADE {}_BUILD ONLY {}_{}".format(
                run_only_build, branch_version, build_number)
        else:
            cc.name = "AWS RF3 UPGRADE {}_{}".format(branch_version, build_number)
        cc.upgrade_base_version = branch_version
        cc.upgrade_base_build = build_number
        cc.minimum_version = minimum_version
        cc.is_odd_release = is_odd_release
        cc.run_only_build = run_only_build
        cc.with_gflags = with_gflags
        return cc

    @staticmethod
    def get_aws_rf3_resize_node():
        cc = CloudConfig.get_aws_rf3()
        cc.upgraded_volume_size = 400
        # cc.minimum_version = "2.7.2.0-b194"
        return cc

    @staticmethod
    def get_gcp_rf3_resize_node():
        cc = CloudConfig.get_gcp_rf3()
        cc.upgraded_volume_size = 400
        # cc.minimum_version = "2.7.2.0-b194"
        return cc

    @staticmethod
    def get_aws_rf3_systemd_universe():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS SYSTEMD RF3"
        cc.use_systemd = True
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_gcp_rf3_systemd_universe():
        cc = CloudConfig.get_gcp_rf3()
        cc.name = "GCP SYSTEMD RF3"
        cc.use_systemd = True
        return cc

    @staticmethod
    def get_aws_airgap_universe():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS AIRGAP"
        cc.airgap_provider = True
        cc.backup_storage_type = BackupStorageType.NFS
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-629a2614",
        }
        cc.custom_az = cc.cloud_provider_region + 'a'
        cc.assign_public_ip = False
        return cc

    @staticmethod
    def get_aws_rf3_nton_tls():
        cc = CloudConfig.get_aws_rf3()
        # enable Node to Node TLS
        cc.name = "AWS NTON TLS"
        cc.enable_nton_tls = True
        cc.verify_health_check = True
        return cc

    @staticmethod
    def get_aws_rf1_cton_tls():
        cc = CloudConfig.get_aws_rf1()
        # enable Node to Node TLS
        cc.name = "AWS CTON TLS"
        cc.enable_nton_tls = True
        cc.enable_cton_tls = True
        return cc

    @staticmethod
    def get_aws_rf3_cloud(cloud_tier, use_cloud_ami=False, graviton=False,
                          minimum_version=None):
        cc = CloudConfig.get_aws_rf3(auth_enabled=True)
        if cloud_tier == "free":
            cc.instance_type = 't3.small'
            cc.replication_factor = 1
            cc.custom_az_mapping = {
                "us-west-2b": "subnet-f840ce9c"
            }
            cc.device_info = {'numVolumes': 1, 'volumeSize': 10, 'storageType': 'GP3',
                              'diskIops': 3000, 'throughput': 125}

        cc.name = f"CLOUD CONFIG AWS {cloud_tier}"
        cc.minimum_version = '2.12.0.0' if minimum_version is None else minimum_version
        if use_cloud_ami:
            # x84_64 2.13.2.0-b41 AMI
            cc.base_cloud_ami = "ami-06f78aeeded2bfca8"
            cc.name = f"CLOUD CONFIG AWS CUSTOM AMI {cloud_tier}"
            cc.is_acceptable_build = cc.is_build_package_available()
            cc.custom_ssh = True
            cc.custom_ssh_user = "centos"

        if graviton:
            cc.cloud_architecture = "aarch64"
            # almalinux AMI for provider
            cc.custom_image_id = "ami-017b11a65709517fe"
            cc.custom_ssh = True
            cc.custom_ssh_user = "ec2-user"
            # aarch64 2.13.2.0-b41 AMI
            cc.base_cloud_ami = "ami-0547586e5bc3fe461"
            cc.instance_type = "t4g.medium"
            if use_cloud_ami:
                cc.name = f"CLOUD CONFIG AWS GRAVITON CUSTOM AMI {cloud_tier}"
            else:
                cc.name = f"CLOUD CONFIG AWS GRAVITON {cloud_tier}"

        cc.use_cloud_ami = cc.base_cloud_ami
        # enable Client to Node TLS
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        # enabling EAR
        cc.enable_enc_at_rest = True
        # config related to cloud
        cc.enable_enc_at_rest = False
        cc.same_rootCA = False
        cc.assign_static_public_ip = True
        cc.cloud_tier = cloud_tier
        cc.verify_health_check = True
        return cc

    @staticmethod
    def get_aws_rf3_cton_tls():
        cc = CloudConfig.get_aws_rf3()
        # enable Client to Node TLS
        cc.name = "AWS CTON TLS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        return cc

    @staticmethod
    def get_aws_hc_vault_tls():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        cc.minimum_version = "2.13.2.0-b118"
        return cc

    @staticmethod
    def get_aws_rf3_enc(minimum_version=None, auth_enabled=False):
        cc = CloudConfig.get_aws_rf3(minimum_version, auth_enabled)
        cc.name = "AWS RF3 ENC"
        cc.enable_enc_at_rest = True
        cc.enable_enc_source = True
        cc.enable_enc_target = True
        cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_aws_rf3_custom_ports():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS RF3 CUSTOM PORTS"
        cc.custom_ports = True
        cc.minimum_version = "2.6.5.0"
        return cc

    @staticmethod
    def get_aws_rf3_io1():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS RF3 IO1"
        cc.device_info = {'numVolumes': 2, 'volumeSize': 250, 'storageType': 'IO1', 'diskIops': 750}
        return cc

    @staticmethod
    def get_aws_rf3_gp3():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS RF3 GP3"
        cc.device_info = {
            'numVolumes': 2,
            'volumeSize': 250,
            'storageType': 'GP3',
            'diskIops': 3000,
            'throughput': 125
        }
        cc.minimum_version = "2.6.0.0"
        return cc

    @staticmethod
    def get_aws_rf3_gp3_ybc():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS RF3 GP3"
        cc.device_info = {
            'numVolumes': 2,
            'volumeSize': 250,
            'storageType': 'GP3',
            'diskIops': 3000,
            'throughput': 125
        }
        cc.minimum_version = "2.15.3.0"
        return cc

    @staticmethod
    def get_aws_rf3_custom_os(os, minimum_version=None):
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS CUSTOM OS RF3 {}".format(os['OS'][:-10])
        cc.custom_ssh = True
        # cc.minimum_version = "2.9.2.0-b1"
        cc.custom_ssh_user = os['ssh_user']
        cc.custom_image_id = os['AMI']
        cc.os = os['OS'][:-10]
        if "ARM" in os['OS']:
            cc.instance_type = 'm6g.medium'
            cc.cloud_architecture = "aarch64"
            cc.is_acceptable_build = cc.is_build_package_available()
        cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_aws_rf1(minimum_version=None, auth_enabled=False):
        cc = CloudConfig.get_aws_rf3(minimum_version=minimum_version,
                                     auth_enabled=auth_enabled)
        cc.name = "AWS RF1"
        cc.replication_factor = 1
        return cc

    @staticmethod
    def get_aws_rf3_yb_net():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS NEW NET"
        cc.custom_vpc_id = CUSTOM_AWS_VPC
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-6553f513",
            "us-west-2b": "subnet-f840ce9c",
            "us-west-2c": "subnet-01ac5b59"
        }
        # Turns out the new us-west-2d does not support c4.large, so testing it in our custom setup.
        cc.instance_type = "c4.large"
        cc.custom_sg_id = "sg-139dde6c"
        return cc

    @staticmethod
    def get_aws_rf3_nfs_backup():
        # NFS endpoints need to be mounted per VPC, so revert back to internal VPC.
        cc = CloudConfig.get_aws_rf3_yb_net()
        cc.name = "AWS RF3 NFS"
        cc.backup_storage_type = BackupStorageType.NFS
        return cc

    @staticmethod
    def get_aws_rf1_nfs_backup():
        cc = CloudConfig.get_aws_rf3_nfs_backup()
        cc.name = "AWS RF1 NFS"
        cc.replication_factor = 1
        cc.download_logs = True
        cc.support_bundle = True
        return cc

    @staticmethod
    def get_aws_multiple_rf(rf1, rf2):
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS RF{} AND RF{}".format(rf1, rf2)
        cc.replication_factor = rf1
        cc.extra_replication_factor = rf2
        if rf1 < rf2:
            cc.enable_enc_at_rest = True
            cc.name += " ENC"
        elif rf1 > rf2:
            cc.enable_enc_at_rest = True
            cc.name += " HC VAULT ENC"
            cc.kms_provider = "HASHICORP"
            cc.minimum_version = "2.11.3.0-b163"
        return cc

    @staticmethod
    def get_aws_rf3_hc_enc():
        cc = CloudConfig.get_aws_rf3()
        cc.enable_enc_at_rest = True
        cc.name = "HC VAULT ENC"
        cc.kms_provider = "HASHICORP"
        cc.minimum_version = "2.11.3.0-b163"
        return cc

    @staticmethod
    def get_aws_nvme():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS NVME"
        cc.instance_type = 'i3.large'
        cc.upgraded_instance_type = 'i3.xlarge'
        cc.device_info['numVolumes'] = 1
        cc.device_info.pop('storageType')
        return cc

    @staticmethod
    def get_aws_arm():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "AWS ARM"
        cc.custom_ssh = True
        cc.custom_ssh_user = "ec2-user"
        # Alma Linux 8 https://wiki.almalinux.org/cloud/AWS.html#community-amis
        cc.custom_image_id = "ami-017b11a65709517fe"
        # enable TLS
        cc.enable_nton_tls = True
        cc.enable_cton_tls = True
        # Changing instance to Graviton Type
        cc.instance_type = 'm6g.medium'
        cc.cloud_architecture = "aarch64"
        cc.minimum_version = "2.11.0.0"
        cc.is_acceptable_build = cc.is_build_package_available()
        return cc

    @staticmethod
    def get_aws_dual_network():
        cc = CloudConfig.get_aws_rf3()
        cc.name = "CLOUD CONFIG AWS DUALNET"
        cc.replication_factor = 1
        cc.use_default_yw = False
        cc.num_nodes = 1
        cc.secondary_subnet_config = {
            "azToSecondarySubnetIds": {
                "us-west-2a": "subnet-07d6873fce9c04d4c"
            }}
        cc.custom_az_mapping = {
            "us-west-2a": "subnet-6553f513"
        }
        cc.minimum_version = "2.13.0.0-b39"
        cc.assign_static_public_ip = True
        cc.cloud_tier = "paid"
        return cc

    @staticmethod
    def get_aws_rf3_small_vol(minimum_version=None):
        cc = CloudConfig.get_aws_rf3()
        cc.device_info = {'numVolumes': 1, 'volumeSize': 15, 'storageType': 'GP2'}
        cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_aws_rf3_dedicated_nodes(min_version=None):
        cc = CloudConfig.get_aws_rf3()
        cc.name = cc.name + " DEDICATED NODES"
        cc.minimum_version = "2.16.0.0-b1"
        cc.dedicated_nodes = True
        if min_version:
            cc.minimum_version = min_version
        return cc

    @staticmethod
    def get_gcp_rf3(auth_enabled=False, min_version=None, multiregion=False):
        cc = CloudConfig()
        cc.name = "GCP RF3"
        cc.cloud_provider_code = 'gcp'
        cc.cloud_provider_region = GCP_LOAD_TESTER_REGION
        cc.async_region = 'us-central1'
        cc.custom_host_vpc_id = CUSTOM_GCE_NETWORK
        cc.custom_vpc_id = CUSTOM_GCE_NETWORK
        cc.instance_type = 'n1-standard-2'
        cc.upgraded_instance_type = 'n1-standard-4'
        cc.device_info = {'numVolumes': 2, 'volumeSize': 250, 'storageType': 'Persistent'}
        cc.replication_factor = 3
        cc.backup_storage_type = BackupStorageType.GCS
        if auth_enabled:
            cc.auth_settings = AuthSettings()
        if min_version is not None:
            cc.minimum_version = min_version
        if multiregion:
            cc.multiregion_univ_config = {
                "us-east1": {"subnetId": "subnet-us-east1"},
                "us-central1": {"subnetId": "subnet-us-central1"},
                "us-west1": {"subnetId": "subnet-us-west1"}
            }
            cc.multiregion_provider_config = cc.multiregion_univ_config.copy()
            cc.async_region = "us-east1"
            cc.name += " MULTIREGION"
        cc._setup_gcp_config_from_env()
        cc.verify_health_check = True
        cc.assign_public_ip = False
        cc.airgap_provider = True
        return cc

    @staticmethod
    def get_gcp_rf3_geo_partition():
        cc = CloudConfig.get_gcp_rf3(auth_enabled=True, multiregion=True)
        cc.name = "GCP 9N RF3"
        cc.num_nodes = 9
        cc.multiregion_provider_config = cc.multiregion_univ_config.copy()
        new_region = {"us-west2": {"subnetId": "subnet-us-west2"}}
        cc.multiregion_provider_config.update(new_region)
        cc.custom_az_mapping = {
            "us-west1-a": "subnet-us-west1-a",
            "us-west1-b": "subnet-us-west1-b",
            "us-west1-c": "subnet-us-west1-c",
            "us-central1-a": "subnet-us-central1-a",
            "us-central1-b": "subnet-us-central1-b",
            "us-central1-c": "subnet-us-central1-c",
            "us-east1-a": "subnet-us-east1-a",
            "us-east1-b": "subnet-us-east1-b",
            "us-east1-c": "subnet-us-east1-c",
        }
        cc.add_new_region = "us-west2"
        cc.default_region = "us-west1"
        cc.remove_new_region = "us-east1"
        # config related to geo-partition
        cc.enable_geo_partition = True
        # enable Client to Node TLS
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        # config related to cloud
        cc.enable_enc_at_rest = False
        cc.same_rootCA = False
        cc.assign_static_public_ip = True
        cc.cloud_tier = "paid"
        return cc

    @staticmethod
    def get_gcp_rf3_vm_image_upgrade():
        cc = CloudConfig.get_gcp_rf3()
        cc.minimum_version = "2.8.0.0-b1"
        cc.rehydration_image = 'projects/centos-cloud/global/images/centos-7-v20210512'
        cc.custom_az = cc.cloud_provider_region + '-b'
        return cc

    @staticmethod
    def get_gcp_rf1(auth_enabled=False):
        cc = CloudConfig.get_gcp_rf3(auth_enabled)
        cc.name = "GCP RF1"
        cc.replication_factor = 1
        return cc

    @staticmethod
    def get_gcp_rf3_enc():
        cc = CloudConfig.get_gcp_rf3()
        cc.enable_enc_at_rest = True
        cc.name += " ENC"
        cc.kms_provider = "GCP"
        cc.minimum_version = "2.15.3.0"
        return cc

    @staticmethod
    def get_ldap():
        cc = CloudConfig.get_gcp_rf3()
        cc.name = "ldap"
        cc.minimum_version = "2.11.2.0-b70"
        return cc

    @staticmethod
    def get_gcp_rf3_nton_tls():
        cc = CloudConfig.get_gcp_rf3()
        # enable Node to Node TLS
        cc.name = "GCP MULTIREGION N TO N TLS"
        cc.enable_nton_tls = True
        cc.download_logs = True
        cc.support_bundle = True
        cc.multiregion_univ_config = {
            "us-east1": {"subnetId": "subnet-us-east1"},
            "us-central1": {"subnetId": "subnet-us-central1"},
            "us-west1": {"subnetId": "subnet-us-west1"}
        }
        cc.multiregion_provider_config = cc.multiregion_univ_config.copy()
        return cc

    def get_upgrade_platform(min_version, upgrade_test_path=None):
        cc = CloudConfig.get_gcp_rf3()
        cc.use_default_yw = False
        if upgrade_test_path is not None:
            cc.name = f"{upgrade_test_path}"
        cc.minimum_version = min_version
        cc.upgrade_test_path = upgrade_test_path
        return cc

    def get_custom_root_k8_platform(min_version, upgrade_test_path=None):
        cc = CloudConfig.get_gcp_rf3()
        cc.use_default_yw = False
        cc.minimum_version = min_version
        return cc

    @staticmethod
    def get_gcp_rf3_cloud(cloud_tier, use_cloud_ami=False, minimum_version=None):
        cc = CloudConfig.get_gcp_rf3(auth_enabled=True)
        if cloud_tier == "free":
            cc.replication_factor = 1
        # enable Client to Node TLS
        cc.name = f"CLOUD CONFIG GCP {cloud_tier}"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        cc.assign_public_ip = True
        cc.airgap_provider = False
        # config related to cloud
        cc.enable_enc_at_rest = False
        cc.same_rootCA = False
        cc.assign_static_public_ip = True
        cc.cloud_tier = cloud_tier
        cc.minimum_version = '2.12.0.0' if minimum_version is None else minimum_version
        if use_cloud_ami:
            cc.base_cloud_ami = "https://www.googleapis.com/compute/v1/projects/" \
                                "sharp-footing-341618/global/images/yugabyte-db-2" \
                                "-13-2-0-b41-centos-958"
            cc.is_acceptable_build = cc.is_build_package_available()
            cc.use_cloud_ami = cc.base_cloud_ami
            cc.name = f"CLOUD CONFIG GCP CUSTOM AMI {cloud_tier}"
        cc.verify_health_check = True
        return cc

    @staticmethod
    def get_gcp_rf3_cton_tls():
        cc = CloudConfig.get_gcp_rf3()
        # enable Client to Node TLS
        cc.name = "GCP CTON TLS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        return cc

    @staticmethod
    def get_gcp_perf():
        cc = CloudConfig.get_gcp_rf3()
        cc.name = "GCP PERF"
        cc.custom_vpc_id = CUSTOM_GCE_NETWORK
        cc.cloud_provider_region = GCP_LOAD_TESTER_REGION
        # TODO(bogdan): async is disabled for now, due to missing support for handling multiple
        # regions and specifying vpcId.
        cc.async_region = None
        cc.instance_type = 'n1-standard-16'  # 'c3.4xlarge' for aws
        cc.upgraded_instance_type = None
        cc.device_info['numVolumes'] = 2
        return cc

    @staticmethod
    def get_gcp_dual_network():
        cc = CloudConfig.get_gcp_rf3()
        cc.use_default_yw = False
        cc.name = "CLOUD CONFIG GCP DUALNET"
        cc.replication_factor = 1
        cc.num_nodes = 1
        cc.secondary_subnet_config = {
            "subnetId": "subnet-us-central1",
            "secondarySubnetId": "yb-subnet-us-central1"
            }
        cc.minimum_version = "2.13.0.0-b39"
        cc.cloud_tier = "paid"
        return cc

    @staticmethod
    def get_gcp_rf3_enc(minimum_version=None, auth_enabled=False, multiregion=False):
        cc = CloudConfig.get_gcp_rf3(auth_enabled, minimum_version, multiregion)
        cc.name = "GCP RF3 ENC"
        cc.enable_enc_at_rest = True
        cc.minimum_version = minimum_version
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_gcp_rf3_custom_os(os):
        cc = CloudConfig.get_gcp_rf3()
        cc.name = "GCP CUSTOM OS RF3 {}".format(os['OS'][:-10])
        cc.custom_ssh = True
        cc.custom_ssh_user = os['ssh_user']
        cc.custom_image_id = os['AMI']
        cc.os = os['OS'][:-10]
        cc.minimum_version = "2.11.2.0-b1"
        return cc

    @staticmethod
    def get_gcp_rf3_dedicated_nodes(min_version=None):
        cc = CloudConfig.get_gcp_rf3()
        cc.name = cc.name + " DEDICATED NODES"
        cc.minimum_version = "2.16.0.0-b1"
        cc.dedicated_nodes = True
        if min_version:
            cc.minimum_version = min_version
        return cc

    @staticmethod
    def get_onprem(min_version=None, manual_provisioning=False):
        cc = CloudConfig()
        cc.cloud_provider_code = 'onprem'
        cc.name = "ONPREM"
        cc.cloud_provider_region = 'us-itest'
        cc.async_region = cc.cloud_provider_region
        cc.instance_type = 'type.large'

        # TODO: should change away from /tmp ?
        cc.device_info = {
            'volumeSize': 40,
            'numVolumes': 2,
            'mountPoints': '/tmp'
        }
        cc.ssh_port = 22
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        if min_version:
            cc.minimum_version = min_version
        # For manually provisioning onprem nodes
        cc.skip_provisioning = manual_provisioning
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_onprem_nton_tls():
        cc = CloudConfig.get_onprem()
        # enable Node to Node TLS
        cc.name = "ONPREM NTON TLS"
        cc.enable_nton_tls = True
        cc.support_bundle = True
        return cc

    @staticmethod
    def get_onprem_cton_tls():
        cc = CloudConfig.get_onprem()
        cc.name = "ONPREM CTON TLS"
        # enable Client to Node TLS
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        cc.download_logs = True
        return cc

    @staticmethod
    def get_onprem_custom_cert():
        cc = CloudConfig.get_onprem()
        cc.replication_factor = 1
        cc.enable_nton_tls = True
        return cc

    # Onprem Provider Configuration for custom home directory
    @staticmethod
    def get_custom_onprem(custom_user, user_privilege="sudo", airgap=False, min_version=None):
        cc = CloudConfig()
        cc.cloud_provider_code = 'onprem'
        cc.name = f"ONPREM CUSTOM {user_privilege}"
        cc.onprem_custom_user = custom_user
        cc.ssh_user_privilege = user_privilege
        if user_privilege == "non-sudo":
            cc.airgap_provider = True
        else:
            cc.airgap_provider = airgap
        cc.onprem_non_sudo = True if user_privilege == "non-sudo" else False
        cc.install_node_exporter = False if user_privilege == "non-sudo" else True
        # For manually provisioning onprem nodes
        cc.skip_provisioning = True
        cc.cloud_provider_region = 'us-itest'
        cc.instance_type = 'type.large'

        # TODO: should change away from /tmp ?
        cc.device_info = {
            'volumeSize': 40,
            'numVolumes': 2,
            'mountPoints': '/tmp'
        }
        cc.ssh_port = 22
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        # For Custom home dir
        cc.home_dir_config = CUSTOM_HOME_DIR_CONFIG if user_privilege == "sudo" else {}
        cc.minimum_version = min_version
        return cc

    @staticmethod
    def get_custom_onprem_tls(custom_user, user_privilege="sudo"):
        cc = CloudConfig.get_custom_onprem(custom_user, user_privilege)
        # enable Client to Node TLS
        cc.name = f"ONPREM CUSTOM {user_privilege} TLS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        return cc

    @staticmethod
    def get_onprem_yba_node_agent(custom_user, user_privilege="sudo", min_version=None):
        cc = CloudConfig.get_custom_onprem(custom_user, user_privilege, min_version=min_version)
        # enable Client to Node TLS
        cc.name = f"ONPREM YBA NODE AGENT {user_privilege} TLS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        cc.custom_ssh_user = custom_user
        return cc

    @staticmethod
    def get_vm_platform(use_yba_installer=False, minimum_version=None):
        cc = CloudConfig()
        cc.use_default_yw = False
        cc.use_yba_installer = use_yba_installer
        cc.minimum_version = minimum_version
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_onprem_vm_platform(use_yba_installer=False, minimum_version=None):
        cc = CloudConfig()
        cc.use_default_yw = False
        cc.use_yba_installer = use_yba_installer
        cc.minimum_version = minimum_version
        cc.cloud_provider_code = 'onprem'
        cc.name = "RF3"
        cc.cloud_provider_region = 'us-itest'
        cc.async_region = cc.cloud_provider_region
        cc.instance_type = 'type.large'

        # TODO: should change away from /tmp ?
        cc.device_info = {
            'volumeSize': 40,
            'numVolumes': 2,
            'mountPoints': '/tmp'
        }
        cc.ssh_port = 22
        cc.replication_factor = 3
        cc.should_download_from_s3 = True
        cc.should_use_remote_package_path = True
        return cc

    @staticmethod
    def get_azu_rf3(auth_enabled=False):
        cc = CloudConfig()
        cc.name = "AZU RF3"
        cc.custom_vpc_id = CUSTOM_AZU_VNET
        cc.cloud_provider_code = 'azu'
        cc.cloud_provider_region = 'westus2'
        cc.instance_type = 'Standard_D2s_v5'
        cc.async_region = 'westus2'
        cc.upgraded_instance_type = 'Standard_D4s_v5'
        cc.device_info = {'numVolumes': 2, 'storageType': 'Premium_LRS', 'volumeSize': 250}
        cc.replication_factor = 3
        cc.custom_az_mapping = {'westus2-1': 'yugabyte-subnet-westus2',
                                'westus2-2': 'yugabyte-subnet-westus2',
                                'westus2-3': 'yugabyte-subnet-westus2'}
        cc.backup_storage_type = BackupStorageType.AZU
        if auth_enabled:
            cc.auth_settings = AuthSettings()
        cc.assign_public_ip = False
        cc._setup_azu_config_from_env()
        return cc

    @staticmethod
    def get_azu_rf1_nton_tls():
        cc = CloudConfig.get_azu_rf1()
        # enable Node to Node TLS
        cc.name = "AZU RF1 NTON TLS"
        cc.enable_nton_tls = True
        cc.support_bundle = True
        return cc

    @staticmethod
    def get_azu_rf1():
        cc = CloudConfig.get_azu_rf3()
        cc.name = "AZU RF1"
        cc.replication_factor = 1
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_azu_rf3_enc(auth_enabled=False):
        cc = CloudConfig.get_azu_rf3(auth_enabled)
        cc.enable_enc_at_rest = True
        cc.name += " ENC"
        cc.kms_provider = "AZU"
        cc.minimum_version = "2.15.3.0"
        return cc

    @staticmethod
    def get_azu_rf3_nton_tls():
        cc = CloudConfig.get_azu_rf3()
        # enable Node to Node TLS
        cc.name = "AZU RF3 NTON TLS"
        cc.enable_nton_tls = True
        return cc

    @staticmethod
    def get_azu_rf3_cton_tls():
        cc = CloudConfig.get_azu_rf3()
        # enable Client to Node TLS
        cc.name = "AZU RF3 CTON TLS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        return cc

    def is_build_package_available(self):
        # Disable cloud AMI and aarch related testcases for dev-itest pipeline
        if "ci" in os.environ.get(ITEST_USER_ENV_VAR, "") and \
           (self.base_cloud_ami or self.cloud_architecture == "aarch64"):
            return False

        return True


class KubernetesCloudConfig(CloudConfig):
    def __init__(self):
        super(KubernetesCloudConfig, self).__init__()
        self._setup_k8s_config_from_env()
        self.cert_manager_type = None

    def get_zonal_config(self):
        base_config = {
            "STORAGE_CLASS": kubernetes.K8S_YUGAWARE_STORAGE_CLASS,
            "OVERRIDES": K8S_PROVIDER_ANNOTATIONS,
            "KUBECONFIG_NAME": "itest_gke.conf",
            "KUBECONFIG_CONTENT": open("/tmp/yugabyte-helm.conf", 'r').read()
        }
        # If cert manager is enabled
        if self.cert_manager_type is not None:
            base_config[f'CERT-MANAGER-{self.cert_manager_type}'] = \
                CERT_MANAGER_METADATA[self.cert_manager_type]
        return base_config

    def _setup_k8s_config_from_env(self):
        # Pull secret and YW Image Registry
        if kubernetes.K8S_ITEST_QUAY_SECRETS_ENV_VAR:
            pull_secret_file = "gcr-pull-secret"
        else:
            pull_secret_file = "yugabyte-k8s-pull-secret"

        self.provider_config = {
            "KUBECONFIG_PROVIDER": "gke",
            "KUBECONFIG_SERVICE_ACCOUNT": "yugabyte-helm",
            "KUBECONFIG_IMAGE_REGISTRY": kubernetes.get_yb_image_registry(),
            "KUBECONFIG_IMAGE_PULL_SECRET_NAME": pull_secret_file,
            "KUBECONFIG_PULL_SECRET_NAME": "itest_pull_secret.yml",
            "KUBECONFIG_PULL_SECRET_CONTENT":
                open(os.environ[kubernetes.K8S_ITEST_SECRETS_ENV_VAR], 'r').read(),
        }

    @staticmethod
    def get_k8s_rf3(minimum_version=None, auth_enabled=False):
        cc = KubernetesCloudConfig()
        cc.name = "K8S RF3"
        cc.cloud_provider_code = 'kubernetes'
        cc.cloud_provider_region = 'us-west-1'
        cc.instance_type = 'xsmall'
        cc.upgraded_instance_type = 'small'
        cc.device_info = {'numVolumes': 2, 'volumeSize': 250}
        cc.replication_factor = 3
        # Kubernetes specific metadata for custom generating the region/az.
        cc.region_metadata = {
            "code": "us-west-1",
            "name": "US West (N. California)",
            "latitude": 37,
            "longitude": -121
        }
        cc.custom_az_mapping = [
            "us-west1-a", "us-west1-b", "us-west1-c"
        ]
        if auth_enabled:
            cc.auth_settings = AuthSettings()
        if minimum_version:
            cc.minimum_version = minimum_version
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_k8s_rf3_itsmall(minimum_version=None, num_volumes=1, volume_size=50):
        cc = KubernetesCloudConfig.get_k8s_rf3(minimum_version)
        cc.instance_type = 'small'
        cc.device_info = {'numVolumes': num_volumes, 'volumeSize': volume_size}
        # Keeping only a single AZ
        cc.custom_az_mapping = cc.custom_az_mapping[0:1]
        return cc

    @staticmethod
    def get_k8s_rf3_upgrade(branch_version, build_number, minimum_version=None,
                            is_odd_release=False, run_only_build=None, with_gflags=False,
                            num_volumes=1, volume_size=50):
        cc = KubernetesCloudConfig.get_k8s_rf3(minimum_version)
        if is_odd_release:
            cc.name = "K8S RF3 ODD UPGRADE {}_{}".format(branch_version, build_number)
        elif run_only_build:
            cc.name = "K8S RF3 ODD UPGRADE {}_BUILD ONLY {}_{}".format(
                run_only_build, branch_version, build_number)
        else:
            cc.name = "K8S RF3 UPGRADE {}_{}".format(branch_version, build_number)
        cc.upgrade_base_version = branch_version
        cc.upgrade_base_build = build_number
        cc.minimum_version = minimum_version
        cc.is_odd_release = is_odd_release
        cc.run_only_build = run_only_build
        cc.device_info = {'numVolumes': num_volumes, 'volumeSize': volume_size}
        return cc

    # Currently unused, only used for faster testing as needed
    @staticmethod
    def get_k8s_rf1():
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8S RF1"
        cc.replication_factor = 1
        # Keeping only a single AZ
        cc.custom_az_mapping = cc.custom_az_mapping[0:1]
        return cc

    @staticmethod
    def get_k8s_storage_rf3():
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8S RF3 STORAGE"
        storageClass = {'storageClass': kubernetes.K8S_YUGAWARE_STORAGE_CLASS}
        cc.device_info.update(storageClass)
        return cc

    @staticmethod
    def get_k8s_rf3_nton_tls():
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.minimum_version = "2.6.0.0-b69"
        # enable Node to Node TLS
        cc.name = "K8S NTON TLS"
        cc.enable_nton_tls = True
        return cc

    @staticmethod
    def get_k8s_rf3_cton_tls():
        cc = KubernetesCloudConfig.get_k8s_rf3()
        # enable Client to Node TLS
        cc.name = "K8S CTON TLS"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        return cc

    @staticmethod
    def get_k8s_rf3_enc(minimum_version=None):
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8S RF3 AWS ENC"
        cc.enable_enc_at_rest = True
        cc.download_logs = True
        cc.support_bundle = True
        cc.minimum_version = minimum_version
        return cc

    @staticmethod
    def get_k8s_rf3_hc_enc():
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8S RF3 HC ENC"
        # Keeping only a single AZ
        cc.custom_az_mapping = cc.custom_az_mapping[0:1]
        cc.enable_enc_at_rest = True
        cc.kms_provider = "HASHICORP"
        cc.minimum_version = "2.11.3.0-b163"
        return cc

    @staticmethod
    def get_k8s_multiple_rf(rf1, rf2):
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8S RF{} and RF{}".format(rf1, rf2)
        cc.replication_factor = rf1
        cc.extra_replication_factor = rf2
        if rf1 != rf2:
            cc.enable_enc_at_rest = True
            cc.name += " AWS ENC"
        cc.validate_metrics = True
        return cc

    @staticmethod
    def get_k8s_hc_vault_tls():
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8s"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        cc.minimum_version = "2.13.2.0-b118"
        return cc

    @staticmethod
    def get_cert_manager(cert_manager_type="CLUSTERISSUER"):
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = f"K8s {cert_manager_type}"
        cc.enable_cton_tls = True
        cc.enable_nton_tls = True
        cc.enable_enc_at_rest = True
        cc.minimum_version = "2.16.0.0-b1"
        cc.cert_manager_type = cert_manager_type
        return cc

    @staticmethod
    def get_k8s_perf():
        # Start off with the default RF3 config.
        cc = KubernetesCloudConfig.get_k8s_rf3()
        cc.name = "K8S PERF"
        # Keeping only a single AZ
        cc.custom_az_mapping = cc.custom_az_mapping[0:1]
        # This is 16vcpu.
        cc.instance_type = "large"
        cc.device_info['numVolumes'] = 2
        return cc
