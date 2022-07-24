import os
import json
import pika
import pika.channel
import pika.spec

from utils import get_logger
from persistence import StorageSystem


class EventIndexer:

    def __init__(self, rabbit_config: dict, mongo_connection_string: str):
        self.logger = get_logger(self.__class__.__name__)

        self.config = rabbit_config
        self.connection = self._create_connection()
        self.storage = StorageSystem(mongo_connection_string)
        self._is_test_run = False
        self._consumer_tag = None

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
        """
        Starts the main logic of the message queue consumer. Will loop infinitely while verifying queue for new messages
        :return: Nothing
        """
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
        self._consumer_tag = channel.basic_consume(queue=self.config['queue_name'],
                                                   on_message_callback=self.on_message_callback)

        self.logger.info('Listening for messages from {} (key: {}). To exit press CTRL+C'.format(
            self.config['queue_name'],
            self.config['routing_key'])
        )
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            channel.stop_consuming()

    def on_message_callback(self,
                            channel: pika.channel.Channel,
                            method_frame: pika.spec.Basic.Deliver,
                            header_frame: pika.spec.BasicProperties,
                            body: bytes) -> None:
        """
        Callback method that is executed whenever a message reaches the queue this system is subscribed to.
        Parameters are those indicated in the official documentation
        https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.basic_consume
        :param channel: MQ channel
        :param method_frame: method frame
        :param header_frame: message properties
        :param body: the actual content/message, as bytes, that was received
        :return: Nothing
        """
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

        if self._is_test_run:
            self.logger.debug("Test run identified, ending")
            channel.basic_cancel(self._consumer_tag)


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
