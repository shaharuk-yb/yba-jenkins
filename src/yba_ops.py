import requests
import os
import time
import logging
import json
import sys
import uuid
import math
import threading

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from utils import CUSTOM_KEYPAIR_PATH, MASTER_GFLAGS, TSERVER_GFLAGS, get_tempfile
from requester import Requester, REQUESTS_TIMEOUT_SEC

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
AWS_PROVIDER_LOCK = threading.Lock()
AWS_PROVIDER_CREATE_WAIT_SEC = 60
GLOBAL_SCOPE_UUID = uuid.UUID(int=0)
TASK_WAIT_TIMEOUT_MINUTES = 60


def get_ips(servers):
    return [ip_port.split(":")[0] for ip_port in servers.split(",")]


def get_gflag_param_string(gflag_dict):
    return str([{"name": dict_key, "value": dict_val}
               for dict_key, dict_val in gflag_dict.items()]).replace("'", '"')


class YBAOps:
    def __init__(self, yba):
        """
        class constructor is used to login to the portal and set the
        class level authToken, customerUUID and userUUID which will be used
        in other operations
        @param yba: this is a YBAnywhere object fetched from db
        """
        # initialize the session object with presets
        retry_strategy = Retry(
            total=3,
            connect=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 400, 501],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        self.request = requests.Session()
        # self.request.headers = self.headers
        self.request.mount("http://", HTTPAdapter(max_retries=retry_strategy))

        self.endpoint = str(yba["endpoint"]).rstrip('/')
        self.protocol = "https" if "https" in self.endpoint else "http"
        self.requester = Requester(self.endpoint.lstrip("http://").lstrip("https://"), 80, self.protocol)
        url = "{}/api/v1/login".format(self.endpoint)
        # get csrf token
        csrf = self.request.get(url, verify=False).cookies.get("csrfCookie")
        req_json = {"email": yba["username"], "password": yba["password"]}
        self.customer_prefix = str(yba["username"]).rstrip("@yugabyte.com")
        self.headers = {
            "content-type": "application/json",
            "Csrf-Token": csrf
        }
        try:
            login_response = self.request.post(url=url, json=req_json, headers=self.headers, verify=False)
        except Exception as e:
            logging.error(e)
            return
        if login_response.status_code not in [200, 201]:
            logging.error(
                f'Problem in accessing of Platform portal with username {yba["username"]} and password {yba["password"]} ')
            return

        login_response = login_response.json()
        self.authToken = login_response['authToken']
        self.customerUUID = login_response['customerUUID']
        self.userUUID = login_response['userUUID']
        self.headers["X-AUTH-TOKEN"] = self.authToken

    def get_universe_uuid(self, universe_name):
        """
        This method is used to get the universe UUID from the portal
        @param universe_name: name of the universe
        @return: universe_uuid
        """
        url = "{}/api/v1/customers/{}/universes".format(self.endpoint, self.customerUUID)
        try:
            universes = self.request.get(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if universes.status_code not in [200, 201]:
            logging.error(
                f'Problem in accessing the details of universes, status-code: {universes.status_code}')
            return None
        universes = universes.json()
        universe_uuid = None
        for universe in universes:
            if universe['name'] == universe_name:
                universe_uuid = universe['universeUUID']
        if universe_uuid is None:
            logging.error(
                f'Universe does not exist with name- {universe_name}')
        return universe_uuid

    def get_universe_uuid_and_url(self, universe_name):
        """
        This method is used to get the universe UUID from the portal
        @param universe_name: name of the universe
        @return: universe_uuid
        """
        url = "{}/api/v1/customers/{}/universes".format(self.endpoint, self.customerUUID)
        try:
            universes = self.request.get(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if universes.status_code not in [200, 201]:
            logging.error(
                f'Problem in accessing the details of universes status-code: {universes.status_code}')
            return
        universes = universes.json()
        universe_uuid = None
        universe_url = None
        for universe in universes:
            if universe['name'] == universe_name:
                universe_uuid = universe['universeUUID']
                universe_url = "{}/universes/{}".format(self.endpoint, universe_uuid)
        if universe_uuid is None:
            logging.error(
                f'Universe does not exist with name - {universe_name}')
        return universe_uuid, universe_url

    def get_universes(self):
        """
        This api returns the list of universes with the uuids
        @return:
        """
        url = "{}/api/v1/customers/{}/universes".format(self.endpoint, self.customerUUID)
        try:
            universes = self.request.get(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if universes.status_code not in [200, 201]:
            logging.error(
                f'Problem in accessing the details of universes')
            return
        universes = universes.json()
        universe_list = []
        for universe in universes:
            if not universe['universeDetails']['universePaused']:
                uni = {'name': universe['name'], 'uuid': universe['universeUUID']}
                universe_list.append(uni)
        return sorted(universe_list, key=lambda i: i['name'])

    def get_storage_uuid(self, storage_type):
        """
        This method is used to get the storage uuid from storage config
        @param storage_type:
        @return: storage_uuid
        """
        url = "{}/api/v1/customers/{}/configs".format(self.endpoint, self.customerUUID)
        try:
            storage_configs = self.request.get(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if storage_configs.status_code not in [200, 201]:
            logging.error(
                f'Problem in accessing the storage configs')
            return
        storage_configs = storage_configs.json()
        storage_uuid = None
        for storage_config in storage_configs:
            if storage_config['name'].lower() == storage_type.lower():
                storage_uuid = storage_config['configUUID']

        if storage_uuid is None:
            logging.error(
                f'Storage Type does not exist')
        return storage_uuid

    def restore_from_backup(self, universe_uuid, storage_uuid, backup_location):
        """
        This method is used to restore a data into the cluster from the backup location specified
        @param universe_uuid: This can be fetched using get_universe_uuid(universe_name)
        @param storage_uuid: This can be fetched using get_storage_uuid(storage_type)
        @param backup_location: This is user input of backup location
        @return: restore_task(Can be used to poll the task status for completion)
        """
        url = "{}/api/v1/customers/{}/universes/{}/backups/restore".format(self.endpoint, self.customerUUID,
                                                                           universe_uuid)
        data = {
            'storageConfigUUID': storage_uuid,
            'storageLocation': backup_location,
            'actionType': 'RESTORE',
            'parallelism': 8,
            'keyspace': 'yugabyte'
        }
        try:
            restore_task = self.request.post(url=url, json=data, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if restore_task.status_code not in [200, 201]:
            logging.error(
                f'Problem in posting the data of restore of universe uuid- {universe_uuid}')
            return
        restore_task = restore_task.json()
        return restore_task['taskUUID']

    def get_universe_nodes(self, universe_uuid):
        """
        This method is used to get the list of nodes from universe and their details
        @param universe_uuid: This can be fetched using get_universe_uuid(universe_name)
        @return: returns a lit of nodes with details, and a clusters info. Example:
        [
            {
                "nodeIdx": 1,
                "nodeName": "yb-15-sagarwal-tpcc-2500-n2",
                "nodeUuid": null,
                "cloudInfo": {
                    "private_ip": "172.151.28.253",
                    "public_ip": "18.236.121.174",
                    "public_dns": "ec2-18-236-121-174.us-west-2.compute.amazonaws.com",
                    "private_dns": "ip-172-151-28-253.us-west-2.compute.internal",
                    "instance_type": "c5d.4xlarge",
                    "subnet_id": "subnet-11798e59",
                    "az": "us-west-2a",
                    "region": "us-west-2",
                    "cloud": "aws",
                    "assignPublicIP": true,
                    "useTimeSync": true,
                    "mount_roots": null
                },
                "azUuid": "99fe90c1-69b2-4d85-83b7-179167cc8b99",
                "placementUuid": "9e89e7b3-2d62-45f0-845d-fa3ba537edab",
                "machineImage": null,
                "state": "Live",
                "isMaster": true,
                "masterHttpPort": 7000,
                "masterRpcPort": 7100,
                "isTserver": true,
                "tserverHttpPort": 9000,
                "tserverRpcPort": 9100,
                "isRedisServer": true,
                "redisServerHttpPort": 11000,
                "redisServerRpcPort": 6379,
                "isYqlServer": true,
                "yqlServerHttpPort": 12000,
                "yqlServerRpcPort": 9042,
                "isYsqlServer": true,
                "ysqlServerHttpPort": 13000,
                "ysqlServerRpcPort": 5433,
                "nodeExporterPort": 9300,
                "cronsActive": true,
                "tserver": true,
                "master": true,
                "ysqlServer": true,
                "redisServer": true,
                "yqlServer": true,
                "allowedActions": [
                    "REMOVE",
                    "STOP",
                    "QUERY"
                ]
            }
        ]
        """
        url = "{}/api/v1/customers/{}/universes/{}".format(self.endpoint, self.customerUUID, universe_uuid)
        try:
            universe = self.request.get(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if universe.status_code not in [200, 201]:
            logging.error(
                f'Problem in accessing the data of universe uuid- {universe_uuid}')
            return
        universe = universe.json()
        return universe['universeDetails']['nodeDetailsSet'], universe['universeDetails']['clusters']

    def get_task_status(self, task_uuid):
        """
        This api is used to get the task status.
        @param task_uuid:
        @return: returns the task status 'Success', 'Failure', 'Running', etc
        """
        for n in range(60):
            url = "{}/api/v1/customers/{}/tasks/{}".format(self.endpoint, self.customerUUID, task_uuid)
            try:
                task_status = self.request.get(url=url, headers=self.headers)
            except Exception as e:
                logging.error(e)
                return None
            if task_status.status_code not in [200, 201]:
                logging.error(
                    f'Problem in getting the status of task with id- {task_uuid} status-code {task_status.status_code}')
                if task_status.status_code in [500, 501, 502, 503, 504, 403]:
                    time.sleep(60)
                    continue
                return None
            task_status = task_status.json()
            return task_status

        return None

    def wait_for_task(self, task_uuid, wait_time, interval):
        # status = self.get_task_status(task_uuid)
        time.sleep(10)
        try:
            for n in range(int(int(wait_time) / interval)):
                resp = self.get_task_status(task_uuid=task_uuid)
                if (resp['status']).lower() == 'running':
                    time.sleep(interval)
                elif (resp['status']).lower() == 'success':
                    return 'success'
                else:
                    return 'failed'
        except Exception as e:
            logging.error(e)
            return

    def wait_for_task_to_delete(self, universe_uuid, wait_time, interval):
        time.sleep(10)
        for n in range(int(int(wait_time) / interval)):
            url = "{}/api/v1/customers/{}/universes".format(self.endpoint, self.customerUUID)
            try:
                universes = self.request.get(url=url, headers=self.headers)
            except Exception as e:
                logging.error(e)
                return
            if universes.status_code not in [200, 201]:
                logging.error(
                    f'Problem in accessing the details of universes')
                return
            universes = universes.json()
            isExist = False
            for universe in universes:
                if universe['universeUUID'] == universe_uuid:
                    isExist = True

            if isExist:
                time.sleep(interval)
            else:
                return "success"
        return "failed"

    def add_yugabyte_release(self, yb_release):
        url = "{}/api/v1/customers/{}/releases".format(self.endpoint, self.customerUUID)
        if ".tar.gz" in yb_release:
            build_path = yb_release

        else:
            build_path = "s3://releases.yugabyte.com/{}/yugabyte-{}-centos-x86_64.tar.gz".format(yb_release, yb_release)
        data = {
            yb_release: {
                "s3": {
                    "accessKeyId": os.environ.get('AWS_ACCESS_KEY'),
                    "secretAccessKey": os.environ.get('AWS_SECRET_KEY'),
                    "paths": {
                        "x86_64": build_path
                    }
                }
            }
        }
        try:
            response = requests.post(url=url, json=data, headers=self.headers)
        except Exception as e:
            return
        return

    def create_universe(self, config_json):
        url = "{}/api/v1/customers/{}/universes".format(self.endpoint, self.customerUUID)
        try:
            response = self.request.post(url=url, json=config_json, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return None, None
        if response.status_code not in [200, 201]:
            logging.error(
                f'Problem in creating the universe, status-code {response.status_code}')
            return None, None
        response = response.json()
        universe_url = "{}/universes/{}".format(self.endpoint, response['universeUUID'])
        return response, universe_url

    def delete_universe(self, universe_name):
        universeUUID = self.get_universe_uuid(universe_name)
        if universeUUID is None:
            logging.error(
                f'Unable to fetch uuid of universe. Problem in deleting the universe with name- {universe_name}')
            return None
        url = "{}/api/v1/customers/{}/universes/{}?isForceDelete=true&isDeleteBackups=false".format(
            self.endpoint, self.customerUUID, universeUUID)
        try:
            response = self.request.delete(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return
        if response.status_code not in [200, 201]:
            logging.error(
                f'Problem in deleting the universe with name- {universe_name} status-code {response.status_code}')
            return None
        response = response.json()
        return response

    def get_master_leader(self, universe_name):
        universeUUID = self.get_universe_uuid(universe_name)
        if universeUUID is None:
            logging.error(
                f'Unable to fetch uuid of universe - {universe_name}')
            return None
        url = "{}/api/v1/customers/{}/universes/{}/leader".format(self.endpoint, self.customerUUID, universeUUID)
        try:
            response = self.request.get(url=url, headers=self.headers)
        except Exception as e:
            logging.error(e)
            return None
        if response.status_code not in [200, 201]:
            logging.error(f'Unable to fetch master_leader of universe - {universe_name}')
            return None
        leader_ip = response.json()['privateIP']
        return leader_ip


    def get_provider_endpoint(self, provider_uuid):
        return '/api/customers/{}/providers/{}'.format(self.userUUID, provider_uuid)

    def get_all_providers(self):
        route = "/api/customers/{}/providers".format(self.userUUID)
        return self.requester.GET(
            route=route, params=None, headers=self.headers)

    def get_all_regions(self, provider_uuid):
        route = "/api/customers/{}/providers/{}/regions".format(self.userUUID, provider_uuid)
        return self.requester.GET(route=route, params=None, headers=self.headers)

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
            route=route, params=region_info, headers=self.headers)

    def create_availability_zone(self, provider_uuid, region_uuid, az):
        route = '{}/regions/{}/zones'.format(self.get_provider_endpoint(provider_uuid), region_uuid)
        azs_to_send = {
            'availabilityZones': [{'code': az, 'name': az}]
        }
        return self.requester.POST(
            route=route, params=azs_to_send, headers=self.headers)

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
            route=route, params=params, headers=self.headers, log_req=True)

    def _create_provider_new(self, params):
        route = "/api/customers/{}/providers".format(self.userUUID)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers, log_req=False)
        if 'taskUUID' in response_json.keys():
            self.wait_for_task_new(response_json['taskUUID'])
        return response_json

    def __create_provider_url(self, yugabyte_version, provider_code):
        if provider_code == 'kubernetes':
            return "/api/customers/{}/providers/kubernetes".format(self.userUUID)
        return "/api/customers/{}/providers".format(self.userUUID)

    def get_or_create_provider(self, provider_config, yugabyte_version, use_latest=False):

        if provider_config['code'] == 'aws':
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
            self.userUUID, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.headers))

    def get_yql_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/yqlservers".format(
            self.userUUID, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.headers))

    def get_master_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/masters".format(
            self.userUUID, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.headers))

    def get_redis_servers(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/redisservers".format(
            self.userUUID, universe_uuid)
        return str(self.requester.GET(route=route, params=None, headers=self.headers))

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
        route = "/api/customers/{}/configs".format(self.userUUID)
        return self.requester.GET(route=route, params=None, headers=self.headers)

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
                route="/api/customers/{}/configs".format(self.userUUID),
                params={
                    'name': cfg_name, 'type': cfg_type, 'data': config,
                    'configName': 'Config-' + str(uuid.uuid4())
                },
                headers=self.headers)
            config_uuid = config_json['configUUID']
        return config_uuid

    def bootstrap_network(self, provider_uuid, cloud_config):
        region_name = cloud_config.cloud_provider_region
        dest_vpc_id = cloud_config.custom_vpc_id
        host_vpc_id = cloud_config.custom_host_vpc_id
        route = "/api/customers/{}/providers/{}/bootstrap".format(self.userUUID,
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
                "keyPairName": self.customer_prefix,
                "sshUser": cloud_config.custom_ssh_user,
                "sshPrivateKeyContent": open(CUSTOM_KEYPAIR_PATH, "r").read()
            })

        response_json = self.requester.POST(
            route=route,
            params=params,
            headers=self.headers
        )
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def get_access_key(self, provider_uuid):
        route = "/api/customers/{}/providers/{}/access_keys".format(
            self.userUUID, provider_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        return response_json

    def get_access_key_code(self, provider_uuid):
        response_json = self.get_access_key(provider_uuid)
        return response_json[0].get('idKey').get('keyCode')

    def add_access_key(self, provider_uuid, params):
        route = "/api/customers/{}/providers/{}/access_keys".format(
            self.userUUID, provider_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)

    def get_provider_private_key(self, provider_uuid):
        response_json = self.get_access_key(provider_uuid)
        if response_json and len(response_json) > 0:
            return response_json[0].get('keyInfo').get('privateKey')
        else:
            return None

    def fetch_platform_version(self):
        route = "/api/v1/app_version"
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        return response_json['version']

    def destroy_provider(self, provider_uuid):
        route = "/api/customers/{}/providers/{}".format(self.userUUID, provider_uuid)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.headers)
        logging.info("Destroy provider returned: {}".format(response_json))
        return response_json


    def get_user_details(self, user_uuid):
        route = '/api/v1/customers/{}/users/{}'.format(self.userUUID, user_uuid)
        return self.requester.GET(route=route, params=None, headers=self.headers)

    def modify_user_role(self, user_uuid, role):
        route = "/api/v1/customers/{}/users/{}?role={}".format(
            self.userUUID, user_uuid, role)
        response = self.requester.PUT(route, None, headers=self.headers, raw=True)
        assert response['success'], "Modify Role failed"

    def get_all_universes(self):
        route = "/api/customers/{}/universes".format(self.userUUID)
        return self.requester.GET(route=route, params=None, headers=self.headers)

    def configure_universe(self, params):
        logging.info("Configuring Universe")
        universe_config = self.requester.POST(
            route="/api/customers/{}/universe_configure".format(self.userUUID),
            params=params, headers=self.headers)
        logging.debug("Configure universe returned: {}.".format(universe_config))
        return universe_config

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
        route = "/api/customers/{}/universe_configure".format(self.userUUID)
        universe_config = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.debug("Configure universe returned: {}, "
                      "editing universe now.".format(universe_config))
        universe.current_nodes = universe_config['nodeDetailsSet']
        route = "/api/customers/{}/universes/{}".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.PUT(
            route=route, params=universe_config, headers=self.headers)
        logging.info("Edit universe returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def do_resize_node(self, universe, params):
        logging.info("Resize node params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/resize_node".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Upgrade returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Resize node done")

    def do_upgrade(self, universe, params):
        logging.info("Upgrade params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Upgrade returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Upgrade done")

    def do_upgrade_gflags(self, universe, params):
        logging.info("Upgrade params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/gflags".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Upgrade Gflags returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Upgrade  Gflags done")

    def do_upgrade_vm(self, universe, params):
        logging.info("Upgrade VM params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/vm".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Upgrade VM returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Upgrade VM done")

    def delete_backups(self, backup_uuid):
        route = "/api/v1/customers/{}/backups".format(self.userUUID)
        params = {
            'backupUUID': backup_uuid
        }
        response_json = self.requester.DELETE(
            route=route, params=params, headers=self.headers)
        logging.info("Delete Backups returned: {}".format(response_json))
        # Wait for the destroy task to complete.
        task_uuid = response_json['taskUUID']
        for task_uuid_itr in task_uuid:
            self.wait_for_task_new(task_uuid_itr, sys._getframe().f_code.co_name)
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
        route = "/api/customers/{}/universe_configure".format(self.userUUID)
        universe_config = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.debug("Configure universe returned: {}, "
                      "adding replica cluster now.".format(universe_config))
        route = "/api/customers/{}/universes/{}/cluster".format(
                self.userUUID, universe_uuid)
        response_json = self.requester.POST(
            route=route, params=universe_config, headers=self.headers)
        logging.info("Add read replica returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)

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
        route = "/api/customers/{}/universe_configure".format(self.userUUID)
        universe_config = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.debug("Configure universe returned: {}, "
                      "editing universe now.".format(universe_config))
        universe.current_nodes = universe_config['nodeDetailsSet']
        route = "/api/customers/{}/universes/{}".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.PUT(
            route=route, params=universe_config, headers=self.headers)
        logging.info("Edit read replica returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def delete_read_replica(self, universe_uuid, cluster_uuid):
        route = "/api/customers/{}/universes/{}/cluster/{}".format(
            self.userUUID, universe_uuid, cluster_uuid)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.headers)
        logging.info("Delete read replica returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)


    def wait_for_task_new(self, task_uuid, description='', timeout=TASK_WAIT_TIMEOUT_MINUTES):
        logging.info("Waiting for completion of task {} UUID {}".format(description, task_uuid))

        route = "/api/customers/{}/tasks/{}".format(self.userUUID, task_uuid)
        route_failure = "/api/customers/{}/tasks/{}/failed".format(self.userUUID, task_uuid)

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
                    route=route, params=None, headers=self.headers, log_req=False)
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
                    route=route_failure, params=None, headers=self.headers)
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
        try:
            logs_keyword = task_uuid
            logs_limit = 5000
            if correlation_id:
                logs_keyword = correlation_id
                logs_limit = 100000
            logging.info(f"Dumping platform logs for keyword {logs_keyword}")
            error_logs = self.requester.GET(
                route=f"/api/v1/logs?maxLines={logs_limit}&queryRegex={logs_keyword}",
                params=None, headers=self.headers, log_req=False
                )
            with open(file_name, 'w') as f:
                f.write(error_logs)
            logging.info(f"Wrote task failure logs to {file_name}")
        except Exception as e:
            logging.debug(f"Unable to write platform logs. ERROR = {e}")

    def get_all_tables(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/tables".format(self.userUUID, universe_uuid)
        return self.requester.GET(route=route, params=None, headers=self.headers)

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
            self.userUUID, universe_uuid, table_uuid)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.headers)
        logging.info("Delete table returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)

    def get_all_backups(self, universe_uuid):
        route = "/api/customers/{}/universes/{}/backups".format(
            self.userUUID, universe_uuid)
        return self.requester.GET(route=route, params=None, headers=self.headers)

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
            self.userUUID, universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        return response_json[universe_uuid]

    def check_and_wait(self, universe_uuid):
        tasks = self.get_task_for_universe(universe_uuid)
        for task in tasks:
            if task["status"] == "Running":
                self.wait_for_task_new(task['id'])

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
            self.userUUID, universe_uuid, table_uuid)
        params = {
            'keyspace': keyspace,
            'tableName': table_name,
            'actionType': 'CREATE',
            'storageConfigUUID': s3_config_uuid,
            'enableVerboseLogs': 'true'
        }
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.headers)
        logging.info("Create table backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created table backup UUID: {}".format(backup_uuid))
        return backup_uuid

    def create_multitable_backup(self, table_type, keyspace, s3_config_uuid, universe_uuid):
        logging.info("Creating {} {} multitable backup".format(table_type, keyspace))

        route = "/api/customers/{}/universes/{}/multi_table_backup".format(
            self.userUUID, universe_uuid)
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
            route=route, params=params, headers=self.headers)
        logging.info("Create multitable backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)
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
        route = "/api/v1/customers/{}/backups".format(self.userUUID)
        params = {
            'universeUUID': universe_uuid,
            'storageConfigUUID': s3_config_uuid,
            'sse': 'false',
            'backupType': table_type,
            'parallelism': 8,
            'keyspaceTableList': keyspace_table_list
        }
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Create backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)
        # adding sleep to ensure backup task is available
        time.sleep(60)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created backup UUID: {}".format(backup_uuid))
        return backup_uuid

    def get_backup_info_v2(self, backup_uuid):
        logging.info("Get details for backup with UUID {}".format(backup_uuid))

        route = "/api/customers/{}/backups/{}".format(
            self.userUUID, backup_uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.headers)
        assert response_json
        return response_json

    def create_schedule_backup(self, table_type, keyspace, s3_config_uuid,
                               universe_uuid, fq, ret, table_name, use_v2_api=False):
        logging.info("Creating {} {} schedule backup".format(table_type, keyspace))
        if use_v2_api:
            route = "/api/v1/customers/{}/create_backup_schedule".format(self.userUUID)
            schedule_name = keyspace + "_schedule_policy"
            params = {
                    "backupType": table_type,
                    "customerUUID": self.userUUID,
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
                                                headers=self.headers)
        else:
            if table_type == 'YQL_TABLE_TYPE':

                table_uuid = self.get_table_uuid(table_type, keyspace, table_name, universe_uuid)
                if not table_uuid:
                    raise RuntimeError("Table {} {}.{} not found.".format(
                        table_type, keyspace, table_name))

                route = "/api/customers/{}/universes/{}/tables/{}/create_backup".format(
                    self.userUUID, universe_uuid, table_uuid)
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
                    self.userUUID, universe_uuid)
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
                route=route, params=params, headers=self.headers)
        logging.info("Create schedule backup returned: {}".format(response_json))
        schedule_uuid = response_json['scheduleUUID']
        return schedule_uuid

    def create_incremental_backup(self, table_type, keyspace, backup_uuid, backup_config_uuid,
                                  universe_uuid):
        logging.info("Creating {} {} incremental backup".format(table_type, keyspace))

        route = "/api/v1/customers/{}/backups".format(self.userUUID)
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
            route=route, params=params, headers=self.headers)
        logging.info("Create incremental backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)
        backup_uuid = self.get_backup_for_task(task_uuid, universe_uuid)
        logging.info("Created table backup UUID: {}".format(backup_uuid))
        return backup_uuid

    def delete_schedule(self, schedule_uuid):
        route = "/api/v1/customers/{}/schedules/{}".format(self.userUUID, schedule_uuid)
        response_json = self.requester.DELETE(
            route=route, params={}, headers=self.headers)
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
            self.userUUID, dest_universe_uuid)
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
            route=route, params=params, headers=self.headers)
        logging.info("Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

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
            self.userUUID, dest_universe_uuid)
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
            route=route, params=params, headers=self.headers)
        logging.info("Multitable Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def restore_allnamespace_backup(self, backup_universe_uuid, s3_config_uuid, backup_uuid,
                                    dest_universe_uuid, use_v2_api=True):

        logging.info("Restoring All Namespace from backup UUID {} (universe {})".format(
            backup_uuid, backup_universe_uuid))

        backup_list = self.get_backup_info(backup_universe_uuid, backup_uuid)['backupList']
        logging.info("Backup List: {}".format(backup_list))

        route = "/api/customers/{}/universes/{}/backups/restore".format(
            self.userUUID, dest_universe_uuid)
        params = {
            'actionType': 'RESTORE',
            'storageConfigUUID': s3_config_uuid,
            'backupList': backup_list,
            'enableVerboseLogs': 'true',
            'parallelism': 8
        }
        if use_v2_api:
            route = "/api/customers/v1/{}/universes/{}/restore".format(
                self.userUUID, dest_universe_uuid)
            params = {
                'actionType': 'RESTORE',
                'storageConfigUUID': s3_config_uuid,
                'backupStorageInfoList': backup_list,
                'enableVerboseLogs': 'true',
                'parallelism': 8
            }

        logging.debug("route: {}, params: {}".format(route, params))
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("All Namespace Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def restore_backup(self, table_type, backup_universe_uuid, backup_uuid, keyspace, table_names,
                       s3_config_uuid, dest_universe_uuid, kms_uuid=None, use_v2_api=True):

        logging.info("Restoring {} {} backup using V2 API".format(table_type, keyspace))
        s3_path = self.get_backup_location(backup_uuid, backup_universe_uuid, use_v2_api)
        logging.info("s3 path: {}".format(s3_path))
        if not s3_path:
            raise RuntimeError("Backup {} was not found.".format(backup_uuid))
        logging.info("Restoring backup from S3 path {}".format(s3_path))
        route = "/api/v1/customers/{}/restore".format(self.userUUID)
        params = {
            'actionType': 'RESTORE',
            'customerUUID': self.userUUID,
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
            route=route, params=params, headers=self.headers)
        logging.info("Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

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
        route = "/api/customers/{}/restore".format(self.userUUID)
        params = {
            'actionType': 'RESTORE',
            'customerUUID': self.userUUID,
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
            route=route, params=params, headers=self.headers)
        logging.info("Restore backup returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def do_node_action(self, universe_uuid, action, node_name, user_intent=None):
        params = {'nodeAction': action}
        # TODO: Allow ASYNC clusterType.
        if user_intent:
            params['clusters'] = [{'clusterType': 'PRIMARY', 'userIntent': user_intent}]
        route = "/api/customers/{}/universes/{}/nodes/{}".format(
            self.userUUID, universe_uuid, node_name)
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.headers)
        logging.info("{} resp {}".format(action, response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def refresh_releases(self):
        route = "/api/customers/{}/releases".format(self.userUUID)
        response_json = self.requester.PUT(
            route=route, params={}, headers=self.headers)
        logging.info("Refreshing releases returned: {}".format(response_json))
        assert response_json['success']

    def add_release(self, version):
        route = "/api/customers/{}/releases".format(self.userUUID)
        response_json = self.requester.POST(
            route=route, params={'version': version}, headers=self.headers)
        logging.info("Adding release for version {} returned: {}".format(version, response_json))
        assert response_json['success']

    def update_release(self, version, state):
        route = "/api/customers/{}/releases/{}".format(self.userUUID, version)
        response_json = self.requester.PUT(
            route=route, params={'state': state}, headers=self.headers)
        logging.info("Updating release {} to {} returned: {}".format(
            version, state, response_json))
        assert response_json

    def get_releases(self):
        route = "/api/customers/{}/releases".format(self.userUUID)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.headers)
        logging.info("Listing release returned: {}".format(response_json))
        assert response_json
        return response_json

    def create_kms_config(self, kms_provider, params):
        route = "/api/customers/{}/kms_configs/{}".format(self.userUUID, kms_provider)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers
        )
        logging.info("Creating KMS Configuration returned: {}".format(response_json))
        assert response_json
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def list_kms_configs(self):
        route = "/api/customers/{}/kms_configs".format(self.userUUID)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.headers
        )
        logging.info("Listing KMS Configuration returned: {}".format(response_json))
        return response_json

    def update_kms_config(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/set_key".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers
        )
        assert response_json

    def get_root_cert(self, cert_uuid):
        route = "/api/customers/{}/certificates/{}/download".format(
            self.userUUID, cert_uuid)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.headers
        )
        assert response_json
        logging.info("Root certificate download request succeeded.")
        # Return a file containing the cert.
        return get_tempfile(response_json['root.crt'])

    def get_tls_certs(self):
        route = "/api/customers/{}/certificates".format(self.userUUID)
        response_json = self.requester.GET(
            route=route, params={}, headers=self.headers
        )
        return response_json

    def get_ysql_cert(self, certs_json):
        cert_uuid = certs_json['uuid']
        route = "/api/customers/{}/certificates/{}".format(self.userUUID, cert_uuid)
        response_json = self.requester.POST(
            route=route, params=certs_json, headers=self.headers)
        assert response_json
        logging.info("yugabytedb certificate download request succeeded.")
        # Return a file containing the cert and key.
        return (
                get_tempfile(response_json['yugabytedb.crt']),
                get_tempfile(response_json['yugabytedb.key'])
               )

    def create_alerts_channel(self, params):
        route = "/api/v1/customers/{}/alert_channels".format(self.userUUID)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Create Alert Channel returned: {}".format(response_json))
        assert response_json
        return response_json

    def create_alerts_destination(self, params):
        route = "/api/v1/customers/{}/alert_destinations".format(self.userUUID)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Create Alerts Destinations returned: {}".format(response_json))
        assert response_json
        return response_json

    def create_alerts_configuration(self, params):
        route = "/api/v1/customers/{}/alert_configurations".format(self.userUUID)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Create Alerts Definitions returned: {}".format(response_json))
        assert response_json
        return response_json

    def edit_alerts_configuration(self, config_uuid, params):
        route = "/api/v1/customers/{}/alert_configurations/{}".format(self.userUUID,
                                                                      config_uuid)
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.headers)
        logging.info("Edit Alerts Definitions returned: {}".format(response_json))
        assert response_json

    def get_active_alerts(self):
        route = "/api/v1/customers/{}/alerts/active".format(self.userUUID)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        # Returns the list of Active Alerts
        return response_json

    def update_tls(self, universe_uuid, params):
        logging.info("Updating TLS for {}".format(universe_uuid))

        route = "/api/customers/{}/universes/{}/update_tls".format(
            self.userUUID, universe_uuid)

        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Update TLS returned: {}".format(response_json))
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name, 90)

    def create_yugabyte_db_release(self, params):
        route = "/api/customers/{}/releases".format(self.userUUID)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)

    def get_master_leader(self, universe_uuid):
        logging.info("Getting Master Leader of {}".format(universe_uuid))
        route = "/api/v1/customers/{}/universes/{}/leader".format(self.userUUID,
                                                                  universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        assert response_json, "Failed to get master leader " \
                              "response for {}".format(route)
        return response_json

    def get_universe_details(self, universe):
        route = "/api/customers/{}/universes/{}".format(
                                                    self.userUUID,
                                                    universe.universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        assert response_json
        return response_json

    def create_xcluster_replication(self, universe, params):
        route = "/api/v1/customers/{}/xcluster_configs".format(
                                                            self.userUUID,
                                                            universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Create xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def edit_xcluster_replication(self, replication_id, params=None):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                            self.userUUID,
                                                            replication_id)
        response_json = self.requester.PUT(
            route=route, params=params, headers=self.headers)
        logging.info("Edit xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def restart_xcluster_replication(self, replication_id, params):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                            self.userUUID,
                                                            replication_id)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Restart xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def delete_xcluster_replication(self, replication_id):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                        self.userUUID,
                                                        replication_id)
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.headers)
        logging.info("Delete xcluster returned: {}".format(response_json))
        assert response_json
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        return response_json

    def get_xcluster_details(self, replication_id):
        route = "/api/v1/customers/{}/xcluster_configs/{}".format(
                                                        self.userUUID,
                                                        replication_id)
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers)
        logging.info("Get xcluster details returned: {}".format(response_json))
        assert response_json
        return response_json

    def check_need_bootstrap(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/need_bootstrap".format(
                                                                self.userUUID,
                                                                universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Need Bootstrap returned: {}".format(response_json))
        assert response_json
        return response_json

    def check_need_xcluster_bootstrap(self, replication_id, params):
        route = "/api/v1/customers/{}/xcluster_configs/{}/need_bootstrap".format(
                                                                self.userUUID,
                                                                replication_id)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Need Bootstrap returned: {}".format(response_json))
        assert response_json
        return response_json

    def add_cert_to_yw(self, params):
        route = "/api/customers/{}/certificates".format(self.userUUID)

        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
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
            route=route, params=None, headers=self.headers)
        server_list, = response_json.values()
        return server_list

    def rolling_restart(self, universe, params):
        logging.info("Rolling Restart params: {}".format(params))
        route = "/api/customers/{}/universes/{}/upgrade/restart".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Rolling restart returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Rolling Restart done")

    def get_health_check(self, universe):
        route = "/api/v1/customers/{}/universes/{}/health_check".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.headers)
        return response_json

    def trigger_health_check(self, universe):
        route = "/api/v1/customers/{}/universes/{}/trigger_health_check".format(
            self.userUUID, universe.universe_uuid)
        logging.info("Health Check Triggered")
        response_json = self.requester.GET(
                route=route, params=None, headers=self.headers)
        return response_json

    def update_health_check_interval(self, intervalMS):
        route = "/api/customers/{}".format(self.userUUID)
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
            route=route, params=params, headers=self.headers)
        logging.info("Updated Health Check Interval: {}".format(response_json))
        return response_json

    def get_slow_queries(self, universe):
        route = "/api/v1/customers/{}/universes/{}/slow_queries".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.headers)
        assert response_json
        return response_json

    def get_cloud_slow_queries(self, universe, username, password):
        user = self.login_user
        headers = user.yb_super_user_headers(username, password)
        route = "/api/v1/customers/{}/universes/{}/slow_queries".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.GET(
            route=route, params=None, headers=headers)
        assert response_json
        return response_json

    def get_live_queries(self, universe):
        route = "/api/v1/customers/{}/universes/{}/live_queries".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.headers)
        assert response_json
        return response_json

    def get_leader_blacklist(self):
        route = "/api/v1/customers/{}/runtime_config/{}/"\
                "key/yb.upgrade.blacklist_leaders".format(
                    self.userUUID, self.userUUID)
        response_json = self.requester.GET(
                route=route, params=None, headers=self.headers)
        logging.info("Leader Blacklist returned: {}".format(response_json))
        assert response_json
        return response_json

    def change_universe_state(self, universe, state):
        route = "/api/v1/customers/{}/universes/{}/{}".format(
            self.userUUID, universe.universe_uuid, state.lower()
        )
        response_json = self.requester.POST(
            route=route, params=None, headers=self.headers)
        task_uuid = response_json['taskUUID']
        self.wait_for_task_new(task_uuid, sys._getframe().f_code.co_name)

    def create_db_credentials(self, universe, params):
        route = "/api/v1/customers/{}/universes/{}/create_db_credentials".format(
            self.userUUID, universe.universe_uuid
        )
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info(response_json)

    def create_support_bundle_for_universe(self, universe_uuid, params):
        route = f"/api/v1/customers/{self.userUUID}/universes/{universe_uuid}/support_bundle"
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers
        )
        logging.info(f"Create Support Bundle returned: {response_json}")
        if "taskUUID" in response_json.keys():
            self.wait_for_task_new(
                response_json["taskUUID"], sys._getframe().f_code.co_name
            )
        return response_json

    def list_support_bundles_from_universe(self, universe_uuid):
        route = f"/api/v1/customers/{self.userUUID}/universes/{universe_uuid}/support_bundle"
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers
        )
        logging.info(f"List all Support Bundles returned: {response_json}")
        return response_json

    def download_support_bundles(self, universe_uuid, bundle_uuid):
        route = \
            f"/api/v1/customers/{self.userUUID}/universes/" \
            f"{universe_uuid}/support_bundle/{bundle_uuid}/download"
        url = f"http://{self.host}:{self.port}{route}"
        response_json = requests.get(
            url=url, headers=self.headers, stream=True)

        return response_json

    def get_support_bundle_from_universe(self, universe_uuid, bundle_uuid):
        route = \
            f"/api/v1/customers/{self.userUUID}/universes/" \
            f"{universe_uuid}/support_bundle/{bundle_uuid}"
        response_json = self.requester.GET(
            route=route, params=None, headers=self.headers
        )
        logging.info(f"Get Support Bundle Returned: {response_json}")
        return response_json

    def delete_support_bundle(self, universe_uuid, bundle_uuid):
        route = \
            f"/api/v1/customers/{self.userUUID}/universes/" \
            f"{universe_uuid}/support_bundle/{bundle_uuid}"
        response_json = self.requester.DELETE(
            route=route, params=None, headers=self.headers
        )
        logging.info(f"Delete Support Bundle Returned: {response_json}")
        return response_json

    def create_geo_partition_tablespace(self, universe, params):
        route = "/api/customers/{}/universes/{}/tablespaces".format(
            self.userUUID, universe.universe_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers
        )
        logging.info(f"Create Tablespace returned: {response_json}")
        if 'taskUUID' in response_json.keys():
            self.wait_for_task_new(response_json['taskUUID'])

    def pre_check_onprem_nodes(self, provider_uuid, instance_ip, params):
        logging.info("Pre Check Onprem node: {}".format(instance_ip))
        route = "/api/customers/{}/providers/{}/instances/{}".format(
            self.userUUID, provider_uuid, instance_ip)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Pre Check Onprem node returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Pre Check Onprem node done for: {}".format(instance_ip))

    def rotate_ssh_key(self, provider_uuid, params):
        logging.info("Rotating SSH key for the provider: {}".format(provider_uuid))
        route = "/api/customers/{}/providers/{}/access_key_rotation".format(
            self.userUUID, provider_uuid)
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("SSH key rotatiom returned: {}".format(response_json))
        # We return a list of tasks here. Waiting for them sequentially
        for task in response_json:
            self.wait_for_task_new(task['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("SSH key rotation done for: {}".format(provider_uuid))

    def delete_pitr_config(self, universe, pitr_config_uuid):
        route = "/api/customers/{}/universes/{}/pitr_config/{}". \
            format(self.userUUID, universe.universe_uuid, pitr_config_uuid)
        response_json = self.requester.DELETE(route=route, params=None,
                                              headers=self.headers)
        assert response_json
        logging.info("Deleted PITR config")
        return response_json

    def list_pitr_configs(self, universe):
        route = "/api/customers/{}/universes/{}/pitr_config". \
            format(self.userUUID, universe.universe_uuid)
        return self.requester.GET(route=route, params=None,
                                  headers=self.headers)

    def create_pitr_config(self, universe, keyspace):
        route = "/api/customers/{}/universes/{}/keyspaces/YSQL/{}/pitr_config". \
            format(self.userUUID, universe.universe_uuid, keyspace)

        params = {'retentionPeriodInSeconds': 604800, 'intervalInSeconds': 86400}

        response_json = self.requester.POST(route=route, params=params,
                                            headers=self.headers)
        assert response_json
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Created PITR config for keyspace {}".format(keyspace))

        return response_json

    def restore_to_a_time(self, universe, pitr_uuid, time_stamp):
        route = "/api/customers/{}/universes/{}/pitr". \
            format(self.userUUID, universe.universe_uuid)

        params = {'restoreTimeInMillis': time_stamp, 'pitrConfigUUID': pitr_uuid}

        response_json = self.requester.POST(route=route, params=params,
                                            headers=self.headers)
        assert response_json
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
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

    def reboot_universe(self, universe):
        route = "/api/customers/{}/universes/{}/upgrade/reboot".format(
            self.userUUID, universe.universe_uuid)
        params = {
            'upgradeOption': "Rolling"
        }
        response_json = self.requester.POST(
            route=route, params=params, headers=self.headers)
        logging.info("Reboot universe returned: {}".format(response_json))
        self.wait_for_task_new(response_json['taskUUID'], sys._getframe().f_code.co_name)
        logging.info("Reboot Universe done")

    def get_yw_api_token(self):
        route = "/api/v1/customers/{}/api_token".format(
            self.userUUID
        )
        response_json = self.requester.PUT(
            route=route, params=None, headers=self.headers
        )
        logging.info(f"Generate YW API Token Returned: {response_json}")
        return response_json
