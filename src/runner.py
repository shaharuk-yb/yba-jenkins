#!/usr/bin/python3
# create yba objects which will utilize the yba api implementations
import logging

from yba_ops import YBAOps
from utils import YBA_BASE_URL, YBA_USERNAME, YBA_PASSWORD, aws_1node_1az_rf1
import time
import uuid


class Runner:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("runner")
        self.yba = YBAOps({"endpoint": YBA_BASE_URL, "username": YBA_USERNAME, "password": YBA_PASSWORD})

    def create_print_delete_universe(self, template):
        # create universe with template
        try:
            self.logger.info("Creating universe with payload")
            res, universe_url = self.yba.create_universe(template)
            self.logger.info("\nUniverse url: {}\n".format(str(universe_url)))
            counter = 0
            while counter < 120 and self.yba.get_task_status(res['taskUUID'])['status'] == 'Running':
                counter += 1
                self.logger.info("Waiting for universe {} creation".format(template["universeUUID"]))
                time.sleep(60)

            task_resp = self.yba.get_task_status(res['taskUUID'])
        except Exception as e:
            self.logger.error("Exception while creating universe")

        # print node details
        self.logger.info(self.yba.get_universe_nodes(template["universeUUID"]))

        # delete universe
        self.yba.delete_universe("sshaikh-test1")
        if self.yba.wait_for_task_to_delete(aws_1node_1az_rf1["universeUUID"], 600, 120) == "success":
            self.logger.info("Successfully deleted the universe")
        else:
            self.logger.info("Failed to delete the universe")

    def modify_template(self, template):
        # set new universe uuid
        template['universeUUID'] = str(uuid.uuid4())

        universe_name = '{}-{}'.format("sshaikh", str(uuid.uuid4().time))

        for node in template['clusters']:
            # modify universe name
            node['userIntent']['universeName'] = universe_name
            node['userIntent']['ybSoftwareVersion'] = "2.17.2.0-b176"
            template['nodePrefix'] = 'yb-dev-{}'.format(universe_name)

        return template


if __name__ == "__main__":
    runner = Runner()
    template = runner.modify_template(aws_1node_1az_rf1)
    runner.create_print_delete_universe(template)
