#!/usr/bin/python3
# create yba objects which will utilize the yba api implementations
import logging

from yba_ops import YBAOps
from utils import YBA_BASE_URL, YBA_USERNAME, YBA_PASSWORD, aws_1node_1az_rf1
import time


class Runner:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("runner")
        self.yba = YBAOps({"endpoint": YBA_BASE_URL, "username": YBA_USERNAME, "password": YBA_PASSWORD})

    def create_print_delete_universe(self):
        # create universe with template
        try:
            self.logger.info("Creating universe with payload")
            res, universe_url = self.yba.create_universe(aws_1node_1az_rf1)
            counter = 0
            while counter < 120 and self.yba.get_task_status(res['taskUUID'])['status'] == 'Running':
                counter += 1
                self.logger.info("Waiting for universe {} creation".format(aws_1node_1az_rf1["universeUUID"]))
                time.sleep(60)

            task_resp = self.yba.get_task_status(res['taskUUID'])
        except Exception as e:
            self.logger.error("Exception while creating universe")

        # print node details
        self.logger.info(self.yba.get_universe_nodes(aws_1node_1az_rf1["universeUUID"]))

        # delete universe
        self.yba.delete_universe("sshaikh-test1")
        if self.yba.wait_for_task_to_delete(aws_1node_1az_rf1["universeUUID"], 600, 120) == "success":
            self.logger.info("Successfully deleted the universe")
        else:
            self.logger.info("Failed to delete the universe")


if __name__ == "__main__":
    runner = Runner()
    runner.create_print_delete_universe()
