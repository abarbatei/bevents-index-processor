import os
import json
import pika

from utils import get_logger
from persistence import StorageSystem


class EventIndexer:

    def __init__(self, rabbit_config, mongo_connection_string):
        self.logger = get_logger(self.__class__.__name__)

        self.config = {
            "host": os.environ["RABBIT_HOST_URL"],
            "port": int(os.environ["RABBIT_HOST_PORT"]),
            "exchange": os.environ["RABBIT_EXCHANGE"],
            "routing_key": os.environ["RABBIT_ROUTING_KEY"],
            'queue_name': os.environ['RABBIT_QUEUE_NAME'],
            "user": os.environ["RABBIT_USER"],
            "password": os.environ["RABBIT_PASSWORD"]
        }

        self.connection = self._create_connection()
        self.storage = StorageSystem(os.environ["MONGO_DB_CONNECTION_STRING"])

    def __del__(self):
        self.connection.close()

    def _create_connection(self) -> pika.BlockingConnection:
        parameters = pika.ConnectionParameters(host=self.config['host'],
                                               port=self.config['port'],
                                               credentials=pika.PlainCredentials(self.config['user'],
                                                                                 self.config['password']),
                                               heartbeat=0)
        return pika.BlockingConnection(parameters)

    def process(self):
        channel = self.connection.channel()
        channel.exchange_declare(exchange=self.config['exchange'],
                                 exchange_type='topic',
                                 durable=True)

        # This method creates or checks a queue
        channel.queue_declare(queue=self.config['queue_name'])

        # Binds the queue to the specified exchange
        channel.queue_bind(queue=self.config['queue_name'],
                           exchange=self.config['exchange'],
                           routing_key=self.config['routing_key'])
        channel.basic_consume(queue=self.config['queue_name'],
                              on_message_callback=self.on_message_callback)

        self.logger.info('Listening for messages from {} (key: {}). To exit press CTRL+C'.format(
            self.config['queue_name'],
            self.config['routing_key'])
        )
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

    def on_message_callback(self, channel, method_frame, header_frame, body):
        try:
            blockchain_event_data = json.loads(body)
            event_name = blockchain_event_data['event_name']
            tx_hash = blockchain_event_data['event_data']['transactionHash']
            self.logger.info("Received new message: {}:{}".format(event_name, tx_hash))

            result = self.storage.insert_event(event_name, blockchain_event_data)
            if not result:
                self.logger.warning("Event {}:{} was discarded from storage".format(event_name, tx_hash))
            else:
                self.logger.info("Event {}:{} successfully saved to storage".format(event_name, tx_hash))
            self.logger.info("Finished processing {} event, discarding from queue".format(event_name))
        except Exception:
            self.logger.exception("Problem inserting event data to database {}, discarding".format(
                body
            ))

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def main():
    rabbit_config = {
        "host": os.environ["RABBIT_HOST_URL"],
        "port": int(os.environ["RABBIT_HOST_PORT"]),
        "exchange": os.environ["RABBIT_EXCHANGE"],
        "routing_key": os.environ["RABBIT_ROUTING_KEY"],
        'queue_name': os.environ['RABBIT_QUEUE_NAME'],
        "user": os.environ["RABBIT_USER"],
        "password": os.environ["RABBIT_PASSWORD"]
    }

    mongo_connection_string = os.environ["MONGO_DB_CONNECTION_STRING"]

    event_indexer = EventIndexer(rabbit_config, mongo_connection_string)
    event_indexer.process()


if __name__ == '__main__':
    main()
