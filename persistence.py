from typing import Optional
from copy import deepcopy
import pymongo

from utils import get_logger, get_utc_time_now


class StorageSystem:
    def __init__(self, mongo_connection_string: str):
        """
        Helper component for database management.
        :param mongo_connection_string: information required to connect to the MongoDB
        """
        self.logger = get_logger(self.__class__.__name__)
        self.events_db = pymongo.MongoClient(mongo_connection_string).events
        self.supported_events = ['PairCreated']

    def insert_event(self, event_name: str, message_data: dict) -> Optional:
        """
        Inserts into the corresponding collection (chosen by event name) a new document with the provided
        message data. Will also add current UNIX timestamp to the document entry.
        :param event_name: the smart contract event name that will be used to determine if event is saved
        :param message_data: content that will be inserted into the database
        :return: Returns None if the event was not added/is not supported or the document id
        of the newly formed db entry
        """
        if event_name not in self.supported_events:
            self.logger.debug("Discarded unsupported event {}".format(event_name))
            return None

        document_data = deepcopy(message_data)
        document_data['t'] = get_utc_time_now()
        insertion_result = self.events_db[event_name].insert_one(document_data)
        return insertion_result.inserted_id
