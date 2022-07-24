import pika

"""
Helper tool for publishing a message on an RabbitMQ topic exchange via a provided routing key
"""


class RabbitPublisher:
    def __init__(self, config):
        self.config = config
        param = pika.ConnectionParameters(host=self.config["host"],
                                          port=self.config["port"],
                                          credentials=pika.PlainCredentials(self.config["user"],
                                                                            self.config["password"]),
                                          heartbeat=0)  # no timeout

        connection = pika.BlockingConnection(param)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.config["exchange"],
                                 exchange_type="topic",
                                 durable=True,
                                 auto_delete=False)
        self.channel = channel

    def publish(self, message, routing_key):

        self.channel.basic_publish(exchange=self.config["exchange"], routing_key=routing_key, body=message)

