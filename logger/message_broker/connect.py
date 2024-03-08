"""
Implementing the message queue functionality using the handler
"""
import os
from message_broker.handlers import rabbitmq
# from config.config import load_env, load_config


class Connection:
    def __init__(self, config) -> None:
        self.config = config

    def create_channel(self):
        # Create a new channle connection with the given parameters
        if self.config.mq.is_url_conn:
            handler = rabbitmq.Client(os.environ.get("MQ_URL"))
            return handler.create()
        else:
            handler = rabbitmq.Client()
            host = os.environ.get("MQ_HOST")
            port = os.environ.get("MQ_PORT")
            user = os.environ.get("MQ_USER", "guest")
            password = os.environ.get("MQ_PASS", "guest")
            handler.generate_url(host=host, port=port,
                                 user=user, password=password)
            return handler.create()

    def subscribe(self, channel, handler, **kwargs):
        channel.queue_declare(
            queue=self.config.mq.unit_test.queue, durable=self.config.mq.unit_test.durable)
        channel.queue_bind(queue=self.config.mq.unit_test.queue,
                           exchange=self.config.mq.unit_test.exchange)
        channel.basic_qos(
            prefetch_count=self.config.mq.unit_test.prefetch.count)
        channel.basic_consume(
            queue=self.config.mq.unit_test.queue, on_message_callback=handler)
