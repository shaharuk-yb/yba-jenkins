# create yba objects which will utilize the yba api implementations
import logging

from yba_ops import YBAOps
from utls import YBA_BASE_URL, YBA_USERNAME, YBA_PASSWORD, UNIVERSE_CREATE_TEMPLATE


if __name__ == "__main__":
    yba = YBAOps({"endpoint": YBA_BASE_URL, "username": YBA_USERNAME, "password": YBA_PASSWORD})
    universes = yba.get_universes()

    print(universes)
    logging.info(universes)
