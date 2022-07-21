import os
import json

from utils import get_logger


class EventIndexer:

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

        config = {
            "host": os.environ["RABBIT_HOST_URL"],
            "port": int(os.environ["RABBIT_HOST_PORT"]),
            "exchange": os.environ["RABBIT_EXCHANGE"],
            "routing_key": os.environ["RABBIT_ROUTING_KEY"],
            "user": os.environ["RABBIT_USER"],
            "password": os.environ["RABBIT_PASSWORD"]
        }

    def process(self):
        pass


def main():
    event_indexer = EventIndexer()
    event_indexer.process()


if __name__ == '__main__':
    main()
