import pymongo

from utils import get_logger, get_utc_time_now


class StorageSystem:
    def __init__(self, mongo_connection_string):
        self.logger = get_logger(self.__class__.__name__)
        self.events_db = pymongo.MongoClient(mongo_connection_string).events
        self.supported_events = ['PairCreated']

    def insert_event(self, event_name, message_data):
        if event_name not in self.supported_events:
            self.logger.debug("Discarded unsupported event {}".format(event_name))
            return None

        message_data['t'] = get_utc_time_now()
        insertion_result = self.events_db[event_name].insert_one(message_data)
        return insertion_result.inserted_id
