import requests
import os
import time
import logging

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


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
        url = "{}/api/v1/login".format(self.endpoint)
        # get csrf token
        csrf = self.request.get(url, verify=False).cookies.get("csrfCookie")
        req_json = {"email": yba["username"], "password": yba["password"]}
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
