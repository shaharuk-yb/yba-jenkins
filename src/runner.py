#!/usr/bin/python3
# create yba objects which will utilize the yba api implementations
import logging

from yba_ops import YBAOps
from utils import YBA_BASE_URL, YBA_USERNAME, YBA_PASSWORD, aws_1node_1az_rf1
import time

if __name__ == "__main__":
    yba = YBAOps({"endpoint": YBA_BASE_URL, "username": YBA_USERNAME, "password": YBA_PASSWORD})
    universes = yba.get_universes()

    print(universes)
    logging.info(universes)

    # create universe with template
    try:
        logging.info("Creating universe with payload")
        res, universe_url = yba.create_universe(aws_1node_1az_rf1)
        counter = 0
        while counter < 120 and yba.get_task_status(res['taskUUID'])['status'] == 'Running':
            counter += 1
            logging.info("Waiting for universe {} creation".format(aws_1node_1az_rf1["universeUUID"]))
            time.sleep(60)

        task_resp = yba.get_task_status(res['taskUUID'])
    except Exception as e:
        logging.error("Exception while creating universe")

    # print node details
    logging.info(yba.get_universe_nodes(aws_1node_1az_rf1["universeUUID"]))

    # delete universe
    yba.delete_universe("sshaikh-test1")
    if yba.wait_for_task_to_delete(aws_1node_1az_rf1["universeUUID"], 600, 120) == "success":
        logging.info("Successfully deleted the universe")
    else:
        logging.info("Failed to delete the universe")
