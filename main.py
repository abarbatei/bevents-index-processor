import os
import json
import pika
import time

from utils import get_logger


class EventIndexer:

    def __init__(self):
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

    def __del__(self):
        self.connection.close()

    def _create_connection(self):
        parameters = pika.ConnectionParameters(host=self.config['host'],
                                               port=self.config['port'],
                                               credentials=pika.PlainCredentials(self.config['user'],
                                                                                 self.config['password']))
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
        blockchain_event_data = json.loads(body)
        self.logger.info("Received new message: {}".format(json.dumps(blockchain_event_data, indent=4)))
        self.logger.info("Finished processing {} event, discarding from queue".format(
            blockchain_event_data['event_name']))
        time.sleep(3)
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)


def main():
    event_indexer = EventIndexer()
    event_indexer.process()


if __name__ == '__main__':
    main()
