# Copyright (c) YugaByte, Inc.

import json
import logging
import os
import requests
import time
from datetime import datetime
import sys
import threading
import uuid
import math

from requests.api import request
from release import is_release_current

from requester import Requester, REQUESTS_TIMEOUT_SEC
from urllib3.exceptions import InsecureRequestWarning


from utils import CUSTOMER_PASSWORD, ITEST_CUSTOM_KEYPAIR_PATH, get_gflag_param_string, \
    MASTER_GFLAGS, TSERVER_GFLAGS, get_ips, TASK_WAIT_TIMEOUT_MINUTES, \
    get_tempfile

CREATE_PROVIDER_UI_VERSION = "2.7.2.0-b172"

SUPER_ADMIN_CUSTOMER_NAME = "super-admin"
# Local YW conf default customer creds.
LOCAL_SUPER_ADMIN_EMAIL = os.environ.get("YW_SUPER_ADMIN_EMAIL", "admin")
LOCAL_SUPER_ADMIN_PASSWORD = os.environ.get("YW_SUPER_ADMIN_PASSWORD", "admin")
AWS_PROVIDER_LOCK = threading.Lock()
AWS_PROVIDER_CREATE_WAIT_SEC = 60
GLOBAL_SCOPE_UUID = uuid.UUID(int=0)


requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)


class LoginUser:
    def __init__(self, user_prefix=None, is_admin=False, is_local=False):
        self.name = "YB Tester"
        self.code = 'itest'

        if is_admin and is_local:
            self.customer_prefix = LOCAL_SUPER_ADMIN_EMAIL.split('@')[0]
            self.email = LOCAL_SUPER_ADMIN_EMAIL
            self.password = LOCAL_SUPER_ADMIN_PASSWORD
        else:
            if user_prefix:
                self.customer_prefix = user_prefix
            else:
                self.customer_prefix = "{}_{}".format(
                    (user_prefix if user_prefix else self.__class__.__name__),
                    datetime.now().strftime('%Y%m%d-%H%M%S'))
            self.email = "{}@yugabyte.com".format(self.customer_prefix)
            self.password = CUSTOMER_PASSWORD

        # To be initialized on login/register.
        self.uuid = None
        self.token = None

    def login(self, uuid, token):
        self.uuid = uuid
        self.token = token

    def is_logged_in(self):
        return self.uuid is not None and self.token is not None

    def logout(self):
        self.uuid = None
        self.token = None

    def headers(self):
        assert self.is_logged_in()
        return {'X-AUTH-TOKEN': self.token}

    def yb_super_user_headers(self, username, password):
        assert self.is_logged_in()
        return {'ysql-username': username,
                'ysql-password': password,
                'X-AUTH-TOKEN': self.token}


SUPER_ADMIN_USER = LoginUser(user_prefix=SUPER_ADMIN_CUSTOMER_NAME, is_admin=True)


def set_super_admin_user(host):
    global SUPER_ADMIN_USER
    if host == 'localhost':
        logging.info("Running on localhost. Using local superuser.")
        SUPER_ADMIN_USER = LoginUser(is_admin=True, is_local=True)
    return SUPER_ADMIN_USER


class YugaWareProxy:
    def __init__(self, host, port, user_prefix=None, login_user=None, protocol=None):
        self.host = host
        self.port = port
        self.protocol = protocol if protocol else "http"
        self.requester = Requester(host, port, self.protocol)
        if login_user is None:
            self.login_user = LoginUser(user_prefix)
        else:
            self.login_user = login_user
        self.test_log_dir = None

    def set_test_log_dir(self, test_log_dir):
        self.test_log_dir = test_log_dir

    def get_provider_endpoint(self, provider_uuid):
        return '/api/customers/{}/providers/{}'.format(self.login_user.uuid, provider_uuid)

    def get_all_providers(self):
        route = "/api/customers/{}/providers".format(self.login_user.uuid)
        return self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())

    def get_all_regions(self, provider_uuid):
        route = "/api/customers/{}/providers/{}/regions".format(self.login_user.uuid, provider_uuid)
        return self.requester.GET(route=route, params=None, headers=self.login_user.headers())

    def get_region_uuid(self, provider_uuid, region_code):
        region_list = list(region_code)
        region_uuid = []
        region_uuid.extend(
            (r['uuid'] for r in self.get_all_regions(provider_uuid) if r['code'] in region_code)
        )
        return region_uuid

    def create_region(self, provider_uuid, region_info):
        route = '{}/regions'.format(self.get_provider_endpoint(provider_uuid))
        return self.requester.POST(
            route=route, params=region_info, headers=self.login_user.headers())

    def create_availability_zone(self, provider_uuid, region_uuid, az):
        route = '{}/regions/{}/zones'.format(self.get_provider_endpoint(provider_uuid), region_uuid)
        azs_to_send = {
            'availabilityZones': [{'code': az, 'name': az}]
        }
        return self.requester.POST(
            route=route, params=azs_to_send, headers=self.login_user.headers())

    def get_provider_uuid(self, provider_code):
        all_providers = self.get_all_providers()
        for provider in all_providers:
            if provider['code'] == provider_code:
                logging.info("Found provider: {}".format(provider))
                return provider['uuid']
        logging.error("Did not find provider with code: {}".format(provider_code))
        return None

    def __create_provider(self, params, yugabyte_version=None):
        route = self.__create_provider_url(yugabyte_version, params['code'])
        return self.requester.POST(
            route=route, params=params, headers=self.login_user.headers(), log_req=True)

    def _create_provider_new(self, params):
        route = "/api/customers/{}/providers".format(self.login_user.uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers(), log_req=False)
        if 'taskUUID' in response_json.keys():
            self.wait_for_task(response_json['taskUUID'])
        return response_json

    def __create_provider_url(self, yugabyte_version, provider_code):
        if provider_code == 'kubernetes':
            return "/api/customers/{}/providers/kubernetes".format(self.login_user.uuid)
        if is_release_current(yugabyte_version, CREATE_PROVIDER_UI_VERSION):
            return "/api/customers/{}/providers/ui".format(self.login_user.uuid)
        return "/api/customers/{}/providers".format(self.login_user.uuid)

    def get_or_create_provider(self, provider_config, yugabyte_version, use_latest=False):

        if provider_config['code'] == 'k8s':
            with AWS_PROVIDER_LOCK:
                logging.info("Lock retrieved.")
                provider_uuid = self._create_provider_new(provider_config)['resourceUUID'] \
                    if use_latest else \
                    self.__create_provider(provider_config, yugabyte_version)['uuid']
                time.sleep(AWS_PROVIDER_CREATE_WAIT_SEC)
            logging.info("Lock released.")
        else:
            provider_uuid = self._create_provider_new(provider_config)['resourceUUID'] \
                if use_latest else self.__create_provider(provider_config, yugabyte_version)['uuid']

        logging.info(f"{provider_config['code']} Provider created: {provider_uuid}")
        return provider_uuid

    def get_ysql_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/ysqlservers".format(
            self.login_user.uuid, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.login_user.headers()))

    def get_yql_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/yqlservers".format(
            self.login_user.uuid, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.login_user.headers()))

    def get_master_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/masters".format(
            self.login_user.uuid, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.login_user.headers()))

    def get_redis_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/redisservers".format(
            self.login_user.uuid, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.login_user.headers()))

    # TODO (ENG-4854): Create API to get pod names directly
    def get_pod_names(self, rf, num_nodes):

        servers = ["yb-master-{}".format(i)
                   for i in range(rf)]
        servers.extend(["yb-tserver-{}".format(i)
                        for i in range(num_nodes)])
        return servers

    def get_servers(self, universe, is_k8s=False):
        if is_k8s:
            private_ips = []
            for node in universe.current_nodes:
                private_ips.append(node['cloudInfo']['private_ip'])
            return private_ips
        else:
            return get_ips(self.get_redis_servers(universe.universe_uuid))

    def get_all_configs(self):
        route = "/api/customers/{}/configs".format(self.login_user.uuid)
        return self.requester.GET(route=route, params=None, headers=self.login_user.headers())

    def get_config_uuid(self, cfg_name, cfg_type):
        all_configs = self.get_all_configs()
        for config in all_configs:
            if config['name'] == cfg_name and config['type'] == cfg_type:
                logging.info("Found {} {} Config: {}".format(
                    cfg_name, cfg_type, config['configUUID']))
                return config['configUUID']
        logging.error("Did not find {} {} Config".format(cfg_name, cfg_type))
        return None

    def get_or_create_config(self, cfg_name, cfg_type, config=None):
        config_uuid = self.get_config_uuid(cfg_name, cfg_type)
        if config_uuid is None:
            logging.info("{} {} Config not found creating one.".format(cfg_name, cfg_type))
            config_json = self.requester.POST(
                route="/api/customers/{}/configs".format(self.login_user.uuid),
                params={
                    'name': cfg_name, 'type': cfg_type, 'data': config,
                    'configName': 'Config-' + str(uuid.uuid4())
                },
                headers=self.login_user.headers())
            config_uuid = config_json['configUUID']
        return config_uuid

    def bootstrap_network(self, provider_uuid, cloud_config):
        region_name = cloud_config.cloud_provider_region
        dest_vpc_id = cloud_config.custom_vpc_id
        host_vpc_id = cloud_config.custom_host_vpc_id
        route = "/api/customers/{}/providers/{}/bootstrap".format(self.login_user.uuid,
                                                                  provider_uuid)

        logging.info("Bootstrapping new Region {}".format(region_name))
        region_info = {}
        if cloud_config.secondary_subnet_config:
            region_info = cloud_config.secondary_subnet_config
        if cloud_config.custom_vpc_id:
            region_info["vpcId"] = cloud_config.custom_vpc_id
            # None of these are relevant if vpcId is not specified.
            if cloud_config.custom_az_mapping:
                region_info["azToSubnetIds"] = cloud_config.custom_az_mapping
            if cloud_config.custom_sg_id:
                region_info["customSecurityGroupId"] = cloud_config.custom_sg_id
            if cloud_config.custom_image_id:
                region_info["customImageId"] = cloud_config.custom_image_id
        params = {
            "hostVpcRegion": region_name,
            "hostVpcId": dest_vpc_id if host_vpc_id is None else host_vpc_id,
            "destVpcId": dest_vpc_id,
            "perRegionMetadata": {
                region_name: region_info
            },
            "airGapInstall": cloud_config.airgap_provider
        }
        # Add the async_region to the set we bootstrap, so YW can know of both regions.
        if cloud_config.async_region and cloud_config.async_region != region_name:
            params["perRegionMetadata"][cloud_config.async_region] = {}
        if cloud_config.multiregion_provider_config is not None:
            params["perRegionMetadata"] = cloud_config.multiregion_provider_config
        if cloud_config.use_cloud_ami is not None:
            params["sshPort"] = cloud_config.ssh_port
        if cloud_config.custom_ssh:
            params.update({
                "keyPairName": self.login_user.customer_prefix,
                "sshUser": cloud_config.custom_ssh_user,
                "sshPrivateKeyContent": open(ITEST_CUSTOM_KEYPAIR_PATH, "r").read()
            })

        response_json = self.requester.POST(
            route=route,
            params=params,
            headers=self.login_user.headers()
        )
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def get_access_key(self, provider_uuid):
        route = "/api/customers/{}/providers/{}/access_keys".format(
            self.login_user.uuid, provider_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        return response_json

    def get_access_key_code(self, provider_uuid):
        response_json = self.get_access_key(provider_uuid)
        return response_json[0].get('idKey').get('keyCode')

    def add_access_key(self, provider_uuid, params):
        route = "/api/customers/{}/providers/{}/access_keys".format(
            self.login_user.uuid, provider_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())

    def get_provider_private_key(self, provider_uuid):
        response_json = self.get_access_key(provider_uuid)
        if response_json and len(response_json) > 0:
            return response_json[0].get('keyInfo').get('privateKey')
        else:
            return None

    def fetch_platform_version(self):
        route = "/api/v1/app_version"
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        return response_json['version']

    def destroy_provider(self, provider_uuid):
        route = "/api/customers/{}/providers/{}".format(self.login_user.uuid, provider_uuid)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.login_user.headers())
        logging.info("Destroy provider returned: {}".format(response_json))
        return response_json

    def login(self, use_headers=True):
        if self.login_user.is_logged_in():
            logging.info("User already logged in with UUID: {} and token: {}".format(
              self.login_user.uuid, self.login_user.token))
            return

        params = {
            'email': self.login_user.email,
            'password': self.login_user.password
        }
        response_json = self.requester.POST(
            route="/api/login", params=params, headers=None, skip_verify=True)
        if 'error' in response_json:
            logging.info("Could not login, registering new user: {} password: {}".format(
                self.login_user.email, self.login_user.password))
            params = {
                'code': self.login_user.code,
                'name': self.login_user.name,
                'email': self.login_user.email,
                'password': self.login_user.password
            }
            headers = SUPER_ADMIN_USER.headers() if use_headers else None
            response_json = self.requester.POST(
                route="/api/register", params=params, headers=headers,
                skip_verify=True)
            # We might be trying to register concurrently, if so, we'll get an error that customer
            # already exists. Try one more time to login, but now fail if we fail.
            if 'error' in response_json:
                response_json = self.requester.POST(
                    route="/api/login", params=params, headers=None)
            logging.info("Registration successful.")
        self.login_user.login(
            response_json["customerUUID"],
            response_json["authToken"])
        logging.info("Login successful. UUID: {} and token {}".format(
            self.login_user.uuid, self.login_user.token))

    def get_user_details(self, user_uuid):
        route = '/api/v1/customers/{}/users/{}'.format(self.login_user.uuid, user_uuid)
        return self.requester.GET(route=route, params=None, headers=self.login_user.headers())

    def modify_user_role(self, user_uuid, role):
        route = "/api/v1/customers/{}/users/{}?role={}".format(
            self.login_user.uuid, user_uuid, role)
        response = self.requester.PUT(route, None, headers=self.login_user.headers(), raw=True)
        assert response['success'], "Modify Role failed"

    def get_all_universes(self):
        route = "/api/customers/{}/universes".format(self.login_user.uuid)
        return self.requester.GET(route=route, params=None, headers=self.login_user.headers())

    def configure_universe(self, params):
        logging.info("Configuring Universe")
        universe_config = self.requester.POST(
            route="/api/customers/{}/universe_configure".format(self.login_user.uuid),
            params=params, headers=self.login_user.headers())
        logging.debug("Configure universe returned: {}.".format(universe_config))
        return universe_config

    def create_universe(self, params, yugabyte_version):
        response_json = self.requester.POST(
            route="/api/customers/{}/universes".format(self.login_user.uuid),
            params=params, headers=self.login_user.headers())
        logging.info("Creating Universe with given params.")
        logging.debug("Create universe with params: {} returned: {}".format(
            params, response_json))
        # Wait for the creation task to complete.
        task_uuid = response_json['taskUUID']
        if is_release_current(yugabyte_version, '2.12.0.0-b1') and \
                params['clusters'][0]['userIntent']['providerType'] != 'kubernetes':
            try:
                self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)
            except Exception as e:
                logging.debug(f"Retrying Create universe task")
                response_json = self.requester.POST(
                    route="/api/customers/{}/tasks/{}".format(self.login_user.uuid, task_uuid),
                    params={}, headers=self.login_user.headers()
                )
                self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        else:
            self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def create_universe_new(self, params):
        response_json = self.requester.POST(
            route="/api/customers/{}/universes/clusters".format(self.login_user.uuid),
            params=params, headers=self.login_user.headers())
        logging.info("Creating Universe with given params.")
        logging.debug("Create universe with params: {} returned: {}".format(
            params, response_json))
        # Wait for the creation task to complete.
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def edit_universe(self, universe, ssh_user="centos"):
        # Configure the universe edit params.
        universe_details = self.get_universe_details(universe)
        params = universe_details['universeDetails']
        params['clusterOperation'] = "EDIT"
        params['clusters'][0]['userIntent'] = universe.custom_user_intent
        params['clusters'][0]['placementInfo'] = universe.placement_info
        params['clusters'][0]['userIntent']['masterGFlags'] = \
            json.loads(get_gflag_param_string(MASTER_GFLAGS))
        params['clusters'][0]['userIntent']['tserverGFlags'] = \
            json.loads(get_gflag_param_string(TSERVER_GFLAGS))
        logging.info("Configuring Universe and Editing the "
                     "Universe: {}".format(universe.universe_uuid))
        route = "/api/customers/{}/universe_configure".format(self.login_user.uuid)
        universe_config = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.debug("Configure universe returned: {}, "
                      "editing universe now.".format(universe_config))
        universe.current_nodes = universe_config['nodeDetailsSet']
        route = "/api/customers/{}/universes/{}".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.PUT(
            route=route, params=universe_config, headers=self.login_user.headers())
        logging.info("Edit universe returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def do_resize_node(self, universe, params):
        logging.info("Resize node params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/resize_node".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Upgrade returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Resize node done")

    def do_upgrade(self, universe, params):
        logging.info("Upgrade params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Upgrade returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Upgrade done")

    def do_upgrade_gflags(self, universe, params):
        logging.info("Upgrade params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/gflags".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Upgrade Gflags returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Upgrade  Gflags done")

    def do_upgrade_vm(self, universe, params):
        logging.info("Upgrade VM params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/vm".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Upgrade VM returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Upgrade VM done")

    def destroy_universe(self, universe_uuid, force_delete="true", delete_backups="true"):
        route = "/api/customers/{}/universes/{}?isForceDelete={}&isDeleteBackups={}".format(
            self.login_user.uuid, universe_uuid, force_delete, delete_backups)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.login_user.headers())
        logging.info("Destroy universe returned: {}".format(response_json))
        # Wait for the destroy task to complete.
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)
        return response_json

    def delete_backups(self, backup_uuid):
        route = "/api/v1/customers/{}/backups".format(self.login_user.uuid)
        params = {
            'backupUUID': backup_uuid
        }
        response_json = self.requester.DELETE(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Delete Backups returned: {}".format(response_json))
        # Wait for the destroy task to complete.
        task_uuid = response_json['taskUUID']
        for task_uuid_itr in task_uuid:
            self.wait_for_task(task_uuid_itr, sys._getframe().f_code.co_name)
        return response_json

    def add_read_replica(self, universe_uuid, user_intent):
        params = {
            'universeUUID': universe_uuid,
            'currentClusterType': 'ASYNC',
            'clusterOperation': 'CREATE',
            'clusters': [{'clusterType': 'ASYNC', 'userIntent': user_intent}]
        }
        logging.info("Configuring Universe: {} and "
                     "Adding read replica".format(universe_uuid))
        route = "/api/customers/{}/universe_configure".format(self.login_user.uuid)
        universe_config = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.debug("Configure universe returned: {}, "
                      "adding replica cluster now.".format(universe_config))
        route = "/api/customers/{}/universes/{}/cluster".format(
                self.login_user.uuid, universe_uuid)
        response_json = self.requester.POST(
            route=route, params=universe_config, headers=self.login_user.headers())
        logging.info("Add read replica returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def edit_read_replica(self, universe):
        # Configure the read only cluster edit params.
        params = {
            'universeUUID': universe.universe_uuid,
            'currentClusterType': 'ASYNC',
            'clusterOperation': 'EDIT',
            'nodeDetailsSet': universe.current_nodes,
            'clusters': [{
                'clusterType': 'ASYNC',
                'userIntent': universe.async_user_intent,
                'placementInfo': universe.async_placement_info,
                'uuid': universe.async_cluster_uuid
            }]
        }
        logging.info("Configuring Universe: {} and "
                     "Editing universe.".format(universe.universe_uuid))
        route = "/api/customers/{}/universe_configure".format(self.login_user.uuid)
        universe_config = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.debug("Configure universe returned: {}, "
                      "editing universe now.".format(universe_config))
        universe.current_nodes = universe_config['nodeDetailsSet']
        route = "/api/customers/{}/universes/{}".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.PUT(
            route=route, params=universe_config, headers=self.login_user.headers())
        logging.info("Edit read replica returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def delete_read_replica(self, universe_uuid, cluster_uuid):
        route = "/api/customers/{}/universes/{}/cluster/{}".format(
            self.login_user.uuid, universe_uuid, cluster_uuid)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.login_user.headers())
        logging.info("Delete read replica returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def wait_for_task(self, task_uuid, description='', timeout=TASK_WAIT_TIMEOUT_MINUTES):
        logging.info("Waiting for completion of task {} UUID {}".format(description, task_uuid))

        route = "/api/customers/{}/tasks/{}".format(self.login_user.uuid, task_uuid)
        route_failure = "/api/customers/{}/tasks/{}/failed".format(self.login_user.uuid, task_uuid)

        wait_iters_count = range(timeout * 2)
        log_freq = math.ceil(wait_iters_count[-1]/10)  # log 10% of iters
        wait_between_iters_secs = 30
        task_state = None
        start = time.time()

        for cur_iter in wait_iters_count:
            status = ""
            correlation_id = None
            try:
                task_state = self.requester.GET(
                    route=route, params=None, headers=self.login_user.headers(), log_req=False)
                status = task_state["status"]
                correlation_id = task_state.get("correlationId", None)
            except requests.exceptions.ConnectTimeout:
                pass

            if status == "Success":
                logging.info("wait_for_task: Success for task {} UUID {} {}s".format(
                    description, task_uuid, (time.time() - start)))
                return

            if status == "Failure" or status == "Aborted":

                self.write_task_logs(f"yw_failed_task_{task_uuid}.log", task_uuid, correlation_id)

                error_json = self.requester.GET(
                    route=route_failure, params=None, headers=self.login_user.headers())
                if error_json is not None:
                    error_strings = [sub_tasks["errorString"]
                                     for sub_tasks in error_json["failedSubTasks"]]
                    all_errors_string = "\n".join(error_strings)
                else:
                    error_msg = "wait_for_task: Could not get failed task list {}s".format(
                        time.time() - start)
                    logging.error(error_msg)
                    raise RuntimeError(error_msg)
                error_msg = "wait_for_task: Failed task with errors in {}s:\n{}".format(
                    time.time() - start, all_errors_string)
                logging.error(error_msg)

                raise RuntimeError(error_msg)

            if 0 == (cur_iter % log_freq):
                # Only log occasionally to avoid log spew
                logging.info("Still waiting for task: {}".format(task_uuid))
            time.sleep(wait_between_iters_secs)

        error_msg = "wait_for_task: Timed out getting task state for {}: {} {}s".format(
            description, task_state, time.time() - start)
        logging.error(error_msg)
        raise RuntimeError(error_msg)

    def write_task_logs(self, file_name, task_uuid, correlation_id):
        if not self.test_log_dir:
            return

        try:
            logs_keyword = task_uuid
            logs_limit = 5000
            if correlation_id:
                logs_keyword = correlation_id
                logs_limit = 100000
            logging.info(f"Dumping platform logs for keyword {logs_keyword}")
            error_logs = self.requester.GET(
                route=f"/api/v1/logs?maxLines={logs_limit}&queryRegex={logs_keyword}",
                params=None, headers=self.login_user.headers(), log_req=False
                )
            with open(os.path.join(self.test_log_dir, file_name), 'w') as f:
                f.write(error_logs)
            logging.info(f"Wrote task failure logs to {file_name}")
        except Exception as e:
            logging.debug(f"Unable to write platform logs. ERROR = {e}")

    def get_all_tables(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/tables".format(self.login_user.uuid, universe_uuid)
        return self.requester.GET(route=route, params=None, headers=self.login_user.headers())

    def get_table_uuid(self, table_type, keyspace, table_name, universe_uuid):
        all_tables = self.get_all_tables(universe_uuid)
        for table in all_tables:
            if table['tableType'] == table_type and \
               table['keySpace'] == keyspace and table['tableName'] == table_name:
                logging.info("Found {} {}.{} Table: {}".format(
                    table_type, keyspace, table_name, table['tableUUID']))
                return table['tableUUID']
        logging.error("Did not find {} {}.{} Table".format(table_type, keyspace, table_name))
        return None

    def delete_table(self, table_type, keyspace, table_name, universe_uuid):
        logging.info("Deleting {} {}.{} table".format(table_type, keyspace, table_name))
        table_uuid = self.get_table_uuid(table_type, keyspace, table_name, universe_uuid)

        if not table_uuid:
            raise RuntimeError("Table {} {}.{} not found.".format(
                table_type, keyspace, table_name))

        route = "/api/customers/{}/universes/{}/tables/{}".format(
            self.login_user.uuid, universe_uuid, table_uuid)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.login_user.headers())
        logging.info("Delete table returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def get_all_backups(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/backups".format(
            self.login_user.uuid, universe_uuid)
        return self.requester.GET(route=route, params=None, headers=self.login_user.headers())

    def get_backup_info(self, universe_uuid, backup_uuid):
        for backup in self.get_all_backups(universe_uuid):
            if backup_uuid == backup['backupUUID']:
                return backup['backupInfo']
        raise RuntimeError(f"No Backup found with uuid {backup_uuid}")

    def get_backup_for_task(self, task_uuid, universe_uuid):
        for backup in self.get_all_backups(universe_uuid):
            if backup['taskUUID'] is None or backup['taskUUID'] == task_uuid:
                return backup['backupUUID']
        return None

    def get_task_for_universe(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/tasks".format(
            self.login_user.uuid, universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        return response_json[universe_uuid]

    def check_and_wait(self, universe_uuid):
        tasks = self.get_task_for_universe(universe_uuid)
        for task in tasks:
            if task["status"] == "Running":
                self.wait_for_task(task['id'])

    def get_specific_tasks_for_universe(self, universe_uuid, task_type):
        all_task = self.get_task_for_universe(universe_uuid)
        task_list = list()
        for task in all_task:
            if task["type"] == task_type:
                task_list.append(task)
        return task_list

    def get_backups_for_schedule(self, schedule_uuid, universe_uuid, use_v2_api=False):
        backup_list = list()
        if use_v2_api:
            for backup in self.get_all_backups(universe_uuid):
                if backup['backupInfo']['backupList'][0]['scheduleUUID'] == schedule_uuid:
                    backup_list.append(backup['backupUUID'])
        else:
            for backup in self.get_all_backups(universe_uuid):
                if backup['scheduleUUID'] == schedule_uuid:
                    backup_list.append(backup['backupUUID'])
        return backup_list

    def get_backup_location(self, backup_uuid, universe_uuid, use_v2_api=False):
        for backup in self.get_all_backups(universe_uuid):
            if backup['backupUUID'] == backup_uuid:
                if use_v2_api:
                    return backup['backupInfo']['backupList'][0]['storageLocation']
                else:
                    return backup['backupInfo']['storageLocation']
        return None

    def create_table_backup(self, table_type, keyspace, table_name, s3_config_uuid, universe_uuid):

        logging.info("Creating {} {}.{} table backup".format(table_type, keyspace, table_name))
        table_uuid = self.get_table_uuid(table_type, keyspace, table_name, universe_uuid)

        if not table_uuid:
            raise RuntimeError("Table {} {}.{} not found.".format(
                table_type, keyspace, table_name))

        route = "/api/customers/{}/universes/{}/tables/{}/create_backup".format(
            self.login_user.uuid, universe_uuid, table_uuid)
        params = {
            'keyspace': keyspace,
            'tableName': table_name,
            'actionType': 'CREATE',
            'storageConfigUUID': s3_config_uuid,
            'enableVerboseLogs': 'true'
        }
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create table backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created table backup UUID: {}".format(backup_uuid))
        return backup_uuid

    def create_multitable_backup(self, table_type, keyspace, s3_config_uuid, universe_uuid):
        logging.info("Creating {} {} multitable backup".format(table_type, keyspace))

        route = "/api/customers/{}/universes/{}/multi_table_backup".format(
            self.login_user.uuid, universe_uuid)
        params = {
            'keyspace': keyspace,
            'actionType': 'CREATE',
            'storageConfigUUID': s3_config_uuid,
            'enableVerboseLogs': 'true',
            'sse': 'false',
            'backupType': table_type,
            'transactionalBackup': 'false',
            'parallelism': 8
        }
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create multitable backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)
        # adding sleep to ensure backup task is available
        time.sleep(60)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created multitable backup UUID: {}".format(backup_uuid))
        return backup_uuid

    # Method to take backup using V2 API
    def create_backup(self, table_type, keyspace, table_name, s3_config_uuid, universe_uuid):
        logging.info("Creating {} {} backup using V2 API".format(table_type, keyspace))

        table_uuids = []
        if keyspace:
            if table_type == 'YQL_TABLE_TYPE' or table_type == 'REDIS_TABLE_TYPE':
                table_uuid = self.get_table_uuid(table_type, keyspace, table_name, universe_uuid)
                if not table_uuid:
                    raise RuntimeError("Table {} {}.{} not found.".format(
                        table_type, keyspace, table_name))
                table_uuids.append(table_uuid)

        keyspace_table_list = [{
                                'keyspace': keyspace,
                                'tableUUIDList': table_uuids
                              }] if keyspace else []
        route = "/api/v1/customers/{}/backups".format(self.login_user.uuid)
        params = {
            'universeUUID': universe_uuid,
            'storageConfigUUID': s3_config_uuid,
            'sse': 'false',
            'backupType': table_type,
            'parallelism': 8,
            'keyspaceTableList': keyspace_table_list
        }
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)
        # adding sleep to ensure backup task is available
        time.sleep(60)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created backup UUID: {}".format(backup_uuid))
        return backup_uuid

    def get_backup_info_v2(self, backup_uuid):
        logging.info("Get details for backup with UUID {}".format(backup_uuid))

        route = "/api/customers/{}/backups/{}".format(
            self.login_user.uuid, backup_uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.login_user.headers())
        assert response_json
        return response_json

    def create_schedule_backup(self, table_type, keyspace, s3_config_uuid,
                               universe_uuid, fq, ret, table_name, use_v2_api=False):
        logging.info("Creating {} {} schedule backup".format(table_type, keyspace))
        if use_v2_api:
            route = "/api/v1/customers/{}/create_backup_schedule".format(self.login_user.uuid)
            schedule_name = keyspace + "_schedule_policy"
            params = {
                    "backupType": table_type,
                    "customerUUID": self.login_user.uuid,
                    "parallelism": 8,
                    "sse": 'false',
                    "storageConfigUUID": s3_config_uuid,
                    "universeUUID": universe_uuid,
                    "keyspaceTableList": [
                        {
                            "keyspace": keyspace
                        }
                    ],
                    "timeBeforeDelete": ret,
                    "expiryTimeUnit": "DAYS",
                    "scheduleName": schedule_name,
                    "schedulingFrequency": fq,
                    "frequencyTimeUnit": "DAYS"
                }
            response_json = self.requester.POST(route=route, params=params,
                                                headers=self.login_user.headers())
        else:
            if table_type == 'YQL_TABLE_TYPE':

                table_uuid = self.get_table_uuid(table_type, keyspace, table_name, universe_uuid)
                if not table_uuid:
                    raise RuntimeError("Table {} {}.{} not found.".format(
                        table_type, keyspace, table_name))

                route = "/api/customers/{}/universes/{}/tables/{}/create_backup".format(
                    self.login_user.uuid, universe_uuid, table_uuid)
                params = {
                    'keyspace': keyspace,
                    'tableName': table_name,
                    'actionType': 'CREATE',
                    'storageConfigUUID': s3_config_uuid,
                    'enableVerboseLogs': 'true',
                    'schedulingFrequency': fq,
                    'timeBeforeDelete': ret,
                    'sse': 'false',
                    'transactionalBackup': 'false',
                    'parallelism': 8
                }
            else:
                route = "/api/customers/{}/universes/{}/multi_table_backup".format(
                    self.login_user.uuid, universe_uuid)
                params = {
                    'keyspace': keyspace,
                    'actionType': 'CREATE',
                    'storageConfigUUID': s3_config_uuid,
                    'enableVerboseLogs': 'true',
                    'schedulingFrequency': fq,
                    'timeBeforeDelete': ret,
                    'sse': 'false',
                    'backupType': table_type,
                    'transactionalBackup': 'false',
                    'parallelism': 8
                }
            response_json = self.requester.PUT(
                route=route, params=params, headers=self.login_user.headers())
        logging.info("Create schedule backup returned: {}".format(response_json))
        schedule_uuid = response_json['scheduleUUID']
        return schedule_uuid

    def create_incremental_backup(self, table_type, keyspace, backup_uuid, backup_config_uuid,
                                  universe_uuid):
        logging.info("Creating {} {} incremental backup".format(table_type, keyspace))

        route = "/api/v1/customers/{}/backups".format(self.login_user.uuid)
        params = {
            'backupType': table_type,
            'storageConfigUUID': backup_config_uuid,
            'universeUUID': universe_uuid,
            'keyspaceTableList': [
                {
                    'keyspace': keyspace
                }
            ],
            'baseBackupUUID': backup_uuid
        }
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create incremental backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created table backup UUID: {}".format(backup_uuid))
        return backup_uuid

    def delete_schedule(self, schedule_uuid):
        route = "/api/v1/customers/{}/schedules/{}".format(self.login_user.uuid, schedule_uuid)
        response_json = self.requester.DELETE(
            route=route, params={}, headers=self.login_user.headers())
        logging.info("Delete schedule returned: {}".format(response_json))
        # Wait for the destroy task to complete.
        success = response_json['success']
        return success

    def restore_table_backup(self, backup_universe_uuid, backup_uuid, keyspace, table_name,
                             s3_config_uuid, dest_universe_uuid, kms_uuid=None, use_v2_api=False):
        logging.info("Restoring {}.{} table from backup UUID {} (universe {})".format(
            keyspace, table_name, backup_uuid, backup_universe_uuid))
        s3_path = self.get_backup_location(backup_uuid, backup_universe_uuid, use_v2_api)
        logging.info("s3 path: {}".format(s3_path))
        if not s3_path:
            raise RuntimeError("Backup {} was not found.".format(backup_uuid))
        logging.info("Restoring backup from S3 path {}".format(s3_path))

        route = "/api/customers/{}/universes/{}/backups/restore".format(
            self.login_user.uuid, dest_universe_uuid)
        params = {
            'keyspace': keyspace,
            'tableName': table_name,
            'actionType': 'RESTORE',
            'storageConfigUUID': s3_config_uuid,
            'storageLocation': s3_path,
            'enableVerboseLogs': 'true'
        }
        if kms_uuid is not None:
            params['kmsConfigUUID'] = kms_uuid
        logging.debug("route: {}, params: {}".format(route, params))
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def restore_multitable_backup(self, backup_universe_uuid, backup_uuid, keyspace,
                                  s3_config_uuid, dest_universe_uuid, kms_uuid=None,
                                  use_v2_api=False):
        logging.info("Restoring {} multitable from backup UUID {} (universe {})".format(
            keyspace, backup_uuid, backup_universe_uuid))
        s3_path = self.get_backup_location(backup_uuid, backup_universe_uuid, use_v2_api)
        if not s3_path:
            raise RuntimeError("Multitable Backup {} was not found.".format(backup_uuid))
        logging.info("Restoring multitable backup from S3 path {}".format(s3_path))

        route = "/api/customers/{}/universes/{}/backups/restore".format(
            self.login_user.uuid, dest_universe_uuid)
        params = {
            'keyspace': keyspace,
            'actionType': 'RESTORE',
            'storageConfigUUID': s3_config_uuid,
            'storageLocation': s3_path,
            'enableVerboseLogs': 'true',
            'parallelism': 8
        }
        if kms_uuid is not None:
            params['kmsConfigUUID'] = kms_uuid
        logging.debug("route: {}, params: {}".format(route, params))
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Multitable Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def restore_allnamespace_backup(self, backup_universe_uuid, s3_config_uuid, backup_uuid,
                                    dest_universe_uuid, use_v2_api=True):

        logging.info("Restoring All Namespace from backup UUID {} (universe {})".format(
            backup_uuid, backup_universe_uuid))

        backup_list = self.get_backup_info(backup_universe_uuid, backup_uuid)['backupList']
        logging.info("Backup List: {}".format(backup_list))

        route = "/api/customers/{}/universes/{}/backups/restore".format(
            self.login_user.uuid, dest_universe_uuid)
        params = {
            'actionType': 'RESTORE',
            'storageConfigUUID': s3_config_uuid,
            'backupList': backup_list,
            'enableVerboseLogs': 'true',
            'parallelism': 8
        }
        if use_v2_api:
            route = "/api/customers/v1/{}/universes/{}/restore".format(
                self.login_user.uuid, dest_universe_uuid)
            params = {
                'actionType': 'RESTORE',
                'storageConfigUUID': s3_config_uuid,
                'backupStorageInfoList': backup_list,
                'enableVerboseLogs': 'true',
                'parallelism': 8
            }

        logging.debug("route: {}, params: {}".format(route, params))
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("All Namespace Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def restore_backup(self, table_type, backup_universe_uuid, backup_uuid, keyspace, table_names,
                       s3_config_uuid, dest_universe_uuid, kms_uuid=None, use_v2_api=True):

        logging.info("Restoring {} {} backup using V2 API".format(table_type, keyspace))
        s3_path = self.get_backup_location(backup_uuid, backup_universe_uuid, use_v2_api)
        logging.info("s3 path: {}".format(s3_path))
        if not s3_path:
            raise RuntimeError("Backup {} was not found.".format(backup_uuid))
        logging.info("Restoring backup from S3 path {}".format(s3_path))
        route = "/api/v1/customers/{}/restore".format(self.login_user.uuid)
        params = {
            'actionType': 'RESTORE',
            'customerUUID': self.login_user.uuid,
            'universeUUID': dest_universe_uuid,
            'storageConfigUUID': s3_config_uuid,
            'enableVerboseLogs': 'true',
            'parallelism': 8,
            'backupStorageInfoList': [
                {
                    'backupType': table_type,
                    'storageLocation': s3_path,
                    'keyspace': keyspace,
                    'tableNameList': table_names
                }
            ]
        }
        if kms_uuid is not None:
            params['kmsConfigUUID'] = kms_uuid
        logging.debug("route: {}, params: {}".format(route, params))
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def restore_incremental_backups(self, table_type, backup_universe_uuid, backup_uuid, keyspace,
                                    s3_config_uuid, dest_universe_uuid, kms_uuid=None):

        logging.info("Restoring {} incremental backup".format(keyspace))
        # logging.info("Restoring {}.{} table from backup UUID {} (universe {})".format(
        #     keyspace, table_name, backup_uuid, backup_universe_uuid))
        s3_path = self.get_backup_location(backup_uuid, backup_universe_uuid, use_v2_api=True)
        logging.info("s3 path: {}".format(s3_path))
        if not s3_path:
            raise RuntimeError("Backup {} was not found.".format(backup_uuid))
        logging.info("Restoring backup from S3 path {}".format(s3_path))
        route = "/api/customers/{}/restore".format(self.login_user.uuid)
        params = {
            'actionType': 'RESTORE',
            'customerUUID': self.login_user.uuid,
            'universeUUID': dest_universe_uuid,
            'storageConfigUUID': s3_config_uuid,
            'backupStorageInfoList': [
                {
                    'backupType': table_type,
                    'storageLocation': s3_path,
                    'keyspace': keyspace,
                }
            ]
        }
        if kms_uuid is not None:
            params['kmsConfigUUID'] = kms_uuid
        logging.debug("route: {}, params: {}".format(route, params))
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def do_node_action(self, universe_uuid, action, node_name, user_intent=None):
        params = {'nodeAction': action}
        # TODO: Allow ASYNC clusterType.
        if user_intent:
            params['clusters'] = [{'clusterType': 'PRIMARY', 'userIntent': user_intent}]
        route = "/api/customers/{}/universes/{}/nodes/{}".format(
            self.login_user.uuid, universe_uuid, node_name)
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("{} resp {}".format(action, response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def refresh_releases(self):
        route = "/api/customers/{}/releases".format(self.login_user.uuid)
        response_json = self.requester.PUT(
            route=route, params={}, headers=self.login_user.headers())
        logging.info("Refreshing releases returned: {}".format(response_json))
        assert response_json['success']

    def add_release(self, version):
        route = "/api/customers/{}/releases".format(self.login_user.uuid)
        response_json = self.requester.POST(
            route=route, params={'version': version}, headers=self.login_user.headers())
        logging.info("Adding release for version {} returned: {}".format(version, response_json))
        assert response_json['success']

    def update_release(self, version, state):
        route = "/api/customers/{}/releases/{}".format(self.login_user.uuid, version)
        response_json = self.requester.PUT(
            route=route, params={'state': state}, headers=self.login_user.headers())
        logging.info("Updating release {} to {} returned: {}".format(
            version, state, response_json))
        assert response_json

    def get_releases(self):
        route = "/api/customers/{}/releases".format(self.login_user.uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.login_user.headers())
        logging.info("Listing release returned: {}".format(response_json))
        assert response_json
        return response_json

    def create_kms_config(self, kms_provider, params):
        route = "/api/customers/{}/kms_configs/{}".format(self.login_user.uuid, kms_provider)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers()
        )
        logging.info("Creating KMS Configuration returned: {}".format(response_json))
        assert response_json
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def list_kms_configs(self):
        route = "/api/customers/{}/kms_configs".format(self.login_user.uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.login_user.headers()
        )
        logging.info("Listing KMS Configuration returned: {}".format(response_json))
        return response_json

    def update_kms_config(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/set_key".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers()
        )
        assert response_json

    def get_root_cert(self, cert_uuid):
        route = "/api/customers/{}/certificates/{}/download".format(
            self.login_user.uuid, cert_uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.login_user.headers()
        )
        assert response_json
        logging.info("Root certificate download request succeeded.")
        # Return a file containing the cert.
        return get_tempfile(response_json['root.crt'])

    def set_global_runtime_config(self, key, value):
        return self.set_runtime_config(GLOBAL_SCOPE_UUID, key, value)

    def delete_global_runtime_config(self, key):
        return self.delete_runtime_config(GLOBAL_SCOPE_UUID, key)

    def set_local_runtime_config(self, key, value):
        return self.set_runtime_config(self.login_user.uuid, key, value)

    def set_runtime_config(self, scope, key, value):
        user = self.login_user
        if str(scope) == str(GLOBAL_SCOPE_UUID):
            user = set_super_admin_user(self.host)

        route = "/api/customers/{}/runtime_config/{}/key/{}".format(
            user.uuid, scope, key)

        headers = user.headers()
        headers['Content-Type'] = 'text/plain'
        response_json = self.requester.PUT(
            route=route, params=value, headers=headers, raw=True
        )

        logging.info("Set runtime config returned: {}".format(response_json))
        return response_json

    def delete_runtime_config(self, scope, key):
        user = self.login_user
        if str(scope) == str(GLOBAL_SCOPE_UUID):
            user = set_super_admin_user(self.host)

        route = "/api/customers/{}/runtime_config/{}/key/{}".format(
            user.uuid, scope, key)

        headers = user.headers()
        response_json = self.requester.DELETE(route, params=None, headers=headers)

        logging.info("Delete runtime config returned: {}".format(response_json))
        return response_json

    def get_tls_certs(self):
        route = "/api/customers/{}/certificates".format(self.login_user.uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.login_user.headers()
        )
        return response_json

    def get_ysql_cert(self, certs_json):
        cert_uuid = certs_json['uuid']
        route = "/api/customers/{}/certificates/{}".format(self.login_user.uuid, cert_uuid)
        response_json = self.requester.POST(
            route=route, params=certs_json, headers=self.login_user.headers())
        assert response_json
        logging.info("yugabytedb certificate download request succeeded.")
        # Return a file containing the cert and key.
        return (
                get_tempfile(response_json['yugabytedb.crt']),
                get_tempfile(response_json['yugabytedb.key'])
               )

    def create_alerts_channel(self, params):
        route = "/api/v1/customers/{}/alert_channels".format(self.login_user.uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create Alert Channel returned: {}".format(response_json))
        assert response_json
        return response_json

    def create_alerts_destination(self, params):
        route = "/api/v1/customers/{}/alert_destinations".format(self.login_user.uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create Alerts Destinations returned: {}".format(response_json))
        assert response_json
        return response_json

    def create_alerts_configuration(self, params):
        route = "/api/v1/customers/{}/alert_configurations".format(self.login_user.uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create Alerts Definitions returned: {}".format(response_json))
        assert response_json
        return response_json

    def edit_alerts_configuration(self, config_uuid, params):
        route = "/api/v1/customers/{}/alert_configurations/{}".format(self.login_user.uuid,
                                                                      config_uuid)
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Edit Alerts Definitions returned: {}".format(response_json))
        assert response_json

    def get_active_alerts(self):
        route = "/api/v1/customers/{}/alerts/active".format(self.login_user.uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        # Returns the list of Active Alerts
        return response_json

    def update_tls(self, universe_uuid, params):
        logging.info("Updating TLS for {}".format(universe_uuid))

        route = "/api/customers/{}/universes/{}/update_tls".format(
            self.login_user.uuid, universe_uuid)

        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Update TLS returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name, 90)

    def create_yugabyte_db_release(self, params):
        route = "/api/customers/{}/releases".format(self.login_user.uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())

    def get_master_leader(self, universe_uuid):
        logging.info("Getting Master Leader of {}".format(universe_uuid))
        route = "/api/v1/customers/{}/universes/{}/leader".format(self.login_user.uuid,
                                                                  universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        assert response_json, "Failed to get master leader " \
                              "response for {}".format(route)
        return response_json

    def get_universe_details(self, universe):
        route = "/api/customers/{}/universes/{}".format(
                                                    self.login_user.uuid,
                                                    universe.universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        assert response_json
        return response_json

    def create_xcluster_replication(self, universe, params):
        route = "/api/v1/customers/{}/xcluster_configs".format(
                                                            self.login_user.uuid,
                                                            universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Create xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def edit_xcluster_replication(self, replication_id, params=None):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                            self.login_user.uuid,
                                                            replication_id)
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Edit xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def restart_xcluster_replication(self, replication_id, params):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                            self.login_user.uuid,
                                                            replication_id)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Restart xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def delete_xcluster_replication(self, replication_id):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                        self.login_user.uuid,
                                                        replication_id)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.login_user.headers())
        logging.info("Delete xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def get_xcluster_details(self, replication_id):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                        self.login_user.uuid,
                                                        replication_id)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        logging.info("Get xcluster details returned: {}".format(response_json))
        assert response_json
        return response_json

    def check_need_bootstrap(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/need_bootstrap".format(
                                                                self.login_user.uuid,
                                                                universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Need Bootstrap returned: {}".format(response_json))
        assert response_json
        return response_json

    def check_need_xcluster_bootstrap(self, replication_id, params):
        route = "/api/v1/customers/{}/xcluster_configs/{}/need_bootstrap".format(
                                                                self.login_user.uuid,
                                                                replication_id)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Need Bootstrap returned: {}".format(response_json))
        assert response_json
        return response_json

    def add_cert_to_yw(self, params):
        route = "/api/customers/{}/certificates".format(self.login_user.uuid)

        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info(params)
        logging.info(str(response_json))
        assert "error" not in str(response_json), "Failed to add the certs to YW"
        return str(response_json)

    def get_tablets_server_data(self, universe):
        leader = self.get_master_leader(universe.universe_uuid)['privateIP']
        route = "/universes/{}/proxy/{}:7000/api/v1/tablet-servers".format(
            universe.universe_uuid, leader
        )
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers())
        server_list, = response_json.values()
        return server_list

    def rolling_restart(self, universe, params):
        logging.info("Rolling Restart params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/restart".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Rolling restart returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Rolling Restart done")

    def get_health_check(self, universe):
        route = "/api/v1/customers/{}/universes/{}/health_check".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.login_user.headers())
        return response_json

    def trigger_health_check(self, universe):
        route = "/api/v1/customers/{}/universes/{}/trigger_health_check".format(
            self.login_user.uuid, universe.universe_uuid)
        logging.info("Health Check Triggered")
        response_json = self.requester.GET(
                route=route, params=None, headers=self.login_user.headers())
        return response_json

    def update_health_check_interval(self, intervalMS):
        route = "/api/customers/{}".format(self.login_user.uuid)
        params = {
            "alertingData": {
                "alertingEmail": "",
                "statusUpdateIntervalMs": 43100000,
                "sendAlertsToYb": 'false',
                "reportOnlyErrors": 'false',
                "checkIntervalMs": intervalMS
                }
        }
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Updated Health Check Interval: {}".format(response_json))
        return response_json

    def get_slow_queries(self, universe):
        route = "/api/v1/customers/{}/universes/{}/slow_queries".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.login_user.headers())
        assert response_json
        return response_json

    def get_cloud_slow_queries(self, universe, username, password):
        user = self.login_user
        headers = user.yb_super_user_headers(username, password)
        route = "/api/v1/customers/{}/universes/{}/slow_queries".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=headers)
        assert response_json
        return response_json

    def get_live_queries(self, universe):
        route = "/api/v1/customers/{}/universes/{}/live_queries".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.login_user.headers())
        assert response_json
        return response_json

    def get_leader_blacklist(self):
        route = "/api/v1/customers/{}/runtime_config/{}/"\
                "key/yb.upgrade.blacklist_leaders".format(
                    self.login_user.uuid, self.login_user.uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.login_user.headers())
        logging.info("Leader Blacklist returned: {}".format(response_json))
        assert response_json
        return response_json

    def login_with_creds(self, username, password):
        params = {
            'email': username,
            'password': password
        }
        response_json = self.requester.POST(
            route="/api/login", params=params, headers=None, skip_verify=True)

        assert "error" not in response_json, f"login failed with {response_json}"

        self.login_user.uuid = response_json["customerUUID"]
        self.login_user.token = response_json["authToken"]
        logging.info(f"LOGGED IN AS {username}")
        return response_json["userUUID"]

    def download_node_logs(self, universe, node_name, node_ip):
        route = "/api/v1/customers/{}/universes/{}/{}/download_logs".format(self.login_user.uuid,
                                                                            universe.universe_uuid,
                                                                            node_name)
        url = "http://{}:{}{}".format(self.host, self.port, route)
        response_json = requests.get(
            url=url, headers=self.login_user.headers(), stream=True)
        assert response_json
        return response_json

    def change_universe_state(self, universe, state):
        route = "/api/v1/customers/{}/universes/{}/{}".format(
            self.login_user.uuid, universe.universe_uuid, state.lower()
        )
        response_json = self.requester.POST(
            route=route, params=None, headers=self.login_user.headers())
        task_uuid = response_json['taskUUID']
        self.wait_for_task(task_uuid, sys._getframe().f_code.co_name)

    def create_db_credentials(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/create_db_credentials".format(
            self.login_user.uuid, universe.universe_uuid
        )
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info(response_json)

    def create_support_bundle_for_universe(self, universe_uuid, params):
        route = f"/api/v1/customers/{self.login_user.uuid}/universes/{universe_uuid}/support_bundle"
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers()
        )
        logging.info(f"Create Support Bundle returned: {response_json}")
        if "taskUUID" in response_json.keys():
            self.wait_for_task(
                response_json["taskUUID"], sys._getframe().f_code.co_name
            )
        return response_json

    def list_support_bundles_from_universe(self, universe_uuid):
        route = f"/api/v1/customers/{self.login_user.uuid}/universes/{universe_uuid}/support_bundle"
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"List all Support Bundles returned: {response_json}")
        return response_json

    def download_support_bundles(self, universe_uuid, bundle_uuid):
        route = \
            f"/api/v1/customers/{self.login_user.uuid}/universes/" \
            f"{universe_uuid}/support_bundle/{bundle_uuid}/download"
        url = f"http://{self.host}:{self.port}{route}"
        response_json = requests.get(
            url=url, headers=self.login_user.headers(), stream=True)

        return response_json

    def get_support_bundle_from_universe(self, universe_uuid, bundle_uuid):
        route = \
            f"/api/v1/customers/{self.login_user.uuid}/universes/" \
            f"{universe_uuid}/support_bundle/{bundle_uuid}"
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Get Support Bundle Returned: {response_json}")
        return response_json

    def delete_support_bundle(self, universe_uuid, bundle_uuid):
        route = \
            f"/api/v1/customers/{self.login_user.uuid}/universes/" \
            f"{universe_uuid}/support_bundle/{bundle_uuid}"
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Delete Support Bundle Returned: {response_json}")
        return response_json

    def create_geo_partition_tablespace(self, universe, params):
        route = "/api/customers/{}/universes/{}/tablespaces".format(
            self.login_user.uuid, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers()
        )
        logging.info(f"Create Tablespace returned: {response_json}")
        if 'taskUUID' in response_json.keys():
            self.wait_for_task(response_json['taskUUID'])

    def pre_check_onprem_nodes(self, provider_uuid, instance_ip, params):
        logging.info("Pre Check Onprem node: {}".format(instance_ip))
        route = "/api/customers/{}/providers/{}/instances/{}".format(
            self.login_user.uuid, provider_uuid, instance_ip)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Pre Check Onprem node returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Pre Check Onprem node done for: {}".format(instance_ip))

    def rotate_ssh_key(self, provider_uuid, params):
        logging.info("Rotating SSH key for the provider: {}".format(provider_uuid))
        route = "/api/customers/{}/providers/{}/access_key_rotation".format(
            self.login_user.uuid, provider_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("SSH key rotatiom returned: {}".format(response_json))
        # We return a list of tasks here. Waiting for them sequentially
        for task in response_json:
            self.wait_for_task(task['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("SSH key rotation done for: {}".format(provider_uuid))

    def delete_pitr_config(self, universe, pitr_config_uuid):
        route = "/api/customers/{}/universes/{}/pitr_config/{}". \
            format(self.login_user.uuid, universe.universe_uuid, pitr_config_uuid)
        response_json = self.requester.DELETE(route=route, params=None,
                                              headers=self.login_user.headers())
        assert response_json
        logging.info("Deleted PITR config")
        return response_json

    def list_pitr_configs(self, universe):
        route = "/api/customers/{}/universes/{}/pitr_config". \
            format(self.login_user.uuid, universe.universe_uuid)
        return self.requester.GET(route=route, params=None,
                                  headers=self.login_user.headers())

    def create_pitr_config(self, universe, keyspace):
        route = "/api/customers/{}/universes/{}/keyspaces/YSQL/{}/pitr_config". \
            format(self.login_user.uuid, universe.universe_uuid, keyspace)

        params = {'retentionPeriodInSeconds': 604800, 'intervalInSeconds': 86400}

        response_json = self.requester.POST(route=route, params=params,
                                            headers=self.login_user.headers())
        assert response_json
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Created PITR config for keyspace {}".format(keyspace))

        return response_json

    def restore_to_a_time(self, universe, pitr_uuid, time_stamp):
        route = "/api/customers/{}/universes/{}/pitr". \
            format(self.login_user.uuid, universe.universe_uuid)

        params = {'restoreTimeInMillis': time_stamp, 'pitrConfigUUID': pitr_uuid}

        response_json = self.requester.POST(route=route, params=params,
                                            headers=self.login_user.headers())
        assert response_json
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info(" Recovered database to time {}".format(time_stamp))

        return response_json

    def verify_response(self, response, log_req=True):
        if response.status_code == 200:
            if log_req:
                logging.debug("Response status: {} for URL {}, reason: {}".format(
                    response.status_code, response.url, response.reason))
            # Check that the response reason is OK.
            if response.reason != 'OK':
                raise RuntimeError("Unexpected response {}.".format(response.reason))
        else:
            logging.error("Response status: {} for URL {}, response: {}".format(
                response.status_code, response.url, response.text))
            raise RuntimeError("Failure response status code {} for URL {}, response: {}".format(
                    response.status_code, response.url, response.text))

    def create_custom_hook(self, body, script):
        route = "http://{}/api/customers/{}/hooks".format(
            self.host, self.login_user.uuid)
        logging.debug("Request POST {}, body: {}".format(route, body))
        file = {"hookFile": open(script, 'rb')}
        response = requests.post(
            route, data=body, files=file, headers=SUPER_ADMIN_USER.headers(),
            timeout=REQUESTS_TIMEOUT_SEC
            )
        self.verify_response(response)
        logging.debug("POST successful, response: {}".format(response.text))
        return json.loads(response.text)

    def create_hook_scope(self, body):
        route = "http://{}/api/customers/{}/hook_scopes".format(
            self.host, self.login_user.uuid)
        logging.debug("Request POST {}, body: {}".format(route, body))
        response = requests.post(
            route, data=body, headers=SUPER_ADMIN_USER.headers(),
            timeout=REQUESTS_TIMEOUT_SEC
            )
        self.verify_response(response)
        logging.debug("POST successful, response: {}".format(response.text))
        return json.loads(response.text)

    def attach_hook_to_hookscope(self, hook_uuid, hs_uuid):
        route = "http://{}/api/customers/{}/hook_scopes/{}/hooks/{}".format(
            self.host, self.login_user.uuid, hs_uuid, hook_uuid)
        logging.debug("Request POST {}".format(route))
        response = requests.post(
            route, headers=SUPER_ADMIN_USER.headers(),
            timeout=REQUESTS_TIMEOUT_SEC
            )
        self.verify_response(response)
        logging.debug("POST successful, response: {}".format(response.text))

    def reboot_universe(self, universe):
        route = "/api/customers/{}/universes/{}/upgrade/reboot".format(
            self.login_user.uuid, universe.universe_uuid)
        params = {
            'upgradeOption': "Rolling"
        }
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers())
        logging.info("Reboot universe returned: {}".format(response_json))
        self.wait_for_task(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Reboot Universe done")

    def get_yw_api_token(self):
        route = "/api/v1/customers/{}/api_token".format(
            self.login_user.uuid
        )
        response_json = self.requester.PUT(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Generate YW API Token Returned: {response_json}")
        return response_json

    def fetch_metrics_status(self, metrics_list):
        for metrics in metrics_list:
            logging.info("Metrics:= {}".format(metrics))
            route = "/api/v1/query?query=" + metrics
            self.requester = Requester(self.host, "9090", self.protocol)
            response_json = self.requester.GET(route=route,
                                               params=None,
                                               headers=self.login_user.headers())
            logging.info("Metrics Response:= {}".format(response_json))
            assert response_json['status'] == "success", "Metrics Response Failed"
        self.requester = Requester(self.host, self.port, self.protocol)

    def start_perf_advisor_run(self, universe):
        route = "/api/v1/customers/{}/universes/{}/start_manually".format(
            self.login_user.uuid, universe.universe_uuid
        )
        response_json = self.requester.POST(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Start perf advisor run returned: {response_json}")
        return response_json

    def verify_perf_advisor_run_status(self, universe):
        route = "/api/v1/customers/{}/universes/{}/last_run".format(
            self.login_user.uuid, universe.universe_uuid
        )
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Verify Perf Advisor run status returned: {response_json}")
        return response_json

    def list_perf_advisor_recommendations(self, params):
        route = "/api/v1/customers/{}/performance_recommendations/page".format(
            self.login_user.uuid
        )
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers()
        )
        logging.info(f"List Perf Advisor recommendations returned: {response_json}")
        return response_json

    def delete_perf_advisor_recommendations(self, params):
        route = "/api/v1/customers/{}/performance_recommendations".format(
            self.login_user.uuid
        )
        response_json = self.requester.DELETE(
            route=route, params=params, headers=self.login_user.headers()
        )
        logging.info(f"Delete Perf Advisor recommendations returned: {response_json}")
        return response_json

    def get_perf_advisor_recommendation_details(self, recommendation_uuid):
        route = "/api/v1/customers/{}/performance_recommendations/{}".format(
            self.login_user.uuid, recommendation_uuid
        )
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Get Perf Advisor recommendations details returned: {response_json}")
        return response_json

    def update_perf_advisor_settings(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/perf_advisor_settings".format(
            self.login_user.uuid, universe.universe_uuid
        )
        response_json = self.requester.POST(
            route=route, params=params, headers=self.login_user.headers()
        )
        logging.info(f"Update Perf Advisor recommendations settings returned: {response_json}")
        return response_json

    def get_perf_advisor_settings(self, universe):
        route = "/api/v1/customers/{}/universes/{}/perf_advisor_settings".format(
            self.login_user.uuid, universe.universe_uuid
        )
        response_json = self.requester.GET(
            route=route, params=None, headers=self.login_user.headers()
        )
        logging.info(f"Get Perf Advisor recommendations settings returned: {response_json}")
        return response_json
