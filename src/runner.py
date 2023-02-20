# create yba objects which will utilize the yba api implementations
import logging

from yba_ops import YBAOps
from utls import YBA_BASE_URL, YBA_USERNAME, YBA_PASSWORD, aws_1node_1az_rf1
import time


if __name__ == "__main__":
    yba = YBAOps({"endpoint": YBA_BASE_URL, "username": YBA_USERNAME, "password": YBA_PASSWORD})
    universes = yba.get_universes()

    print(universes)
    logging.info(universes)

    # create universe with template
    try:
        res, universe_url = yba.create_universe(aws_1node_1az_rf1)
        counter = 0
        while counter < 120 and yba.get_task_status(res['taskUUID'])['status'] == 'Running':
            counter += 1
            time.sleep(60)

        task_resp = yba.get_task_status(res['taskUUID'])
    except Exception as e:
        logging.error("Exception while creating universe")

    # delete universe
    yba.delete_universe("sshaikh-test1")
    if yba.wait_for_task_to_delete(aws_1node_1az_rf1["universeUUID"], 600, 120) == "success":
        logging.info("Successfully deleted the universe")
    else:
        logging.info("Failed to delete the universe")


