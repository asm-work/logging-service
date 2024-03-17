"""RabbitMQ message handler: Concrete implementations"""

import os

import pika
from dotmap import DotMap
from message.handlers.base_handler import MessageHandler
from message_broker.handlers.base_handler import (
    BaseMessageHandler,
    ChannelCallbackHandler,
    ChannelHandler,
    ConnectionCallbackHandler,
    ConnectionHandler,
    ConnectionMethod,
    ConsumerCallbackHandler,
    ConsumerHandler,
    ExchangeCallbackHandler,
    ExchangeHandler,
    QueueCallbackHandler,
    QueueHandler,
)
from pika.channel import Channel as PikaChannel
from utils.constants import Config
from utils.logger import LoggingHandler


class Client(BaseMessageHandler):

    def __init__(self, url=None):
        self.url = url

    def generate_url(self, **kwargs):
        host = kwargs.get("host")
        port = kwargs.get("port")
        user = kwargs.get("user")
        password = kwargs.get("password")
        self.url = f"amqp://{user}:{password}@{host}:{port}/"

    def create(self):
        parms = pika.URLParameters(self.url)
        connection = pika.BlockingConnection(parms)
        return connection.channel()


class URLMethod(ConnectionMethod):

    def __init__(self, config: DotMap):
        self.config = config

    def get_url(self) -> str | None:
        if self.config.mq.is_url_conn:
            return os.environ.get(Config.MQ_URL.value)
        # TODO: add exception handler


class ParameterMethod(ConnectionMethod):

    def __init__(self, config: DotMap):
        self.config = config

    def get_url(self) -> str | None:
        # TODO: Give the provision for other advanced parameters
        host = os.environ.get(Config.MQ_HOST.value, Config.MQ_HOST_DEFAULT.value)
        port = os.environ.get(Config.MQ_PORT.value, Config.MQ_PORT_DEFAULT.value)
        user = os.environ.get(Config.MQ_USER.value, Config.MQ_USER_DEFAULT.value)
        pswd = os.environ.get(Config.MQ_PASS.value, Config.MQ_PASS_DEFAULT.value)
        return f"amqp://{user}:{pswd}@{host}:{port}/"


class ConnectionCallback(ConnectionCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the connection
        self.consumer.connection.set_callback(self)

    def on_connection_open(self, connection: pika.SelectConnection):
        self._logger.info("Connection opened")
        self.consumer.channel.open_channel(connection)

    def on_connection_closed(self, _connection, reason: str):
        self.consumer.channel.set_empty_channel()
        if self.consumer.is_closing():
            self.consumer.stop_ioloop()
        else:
            self._logger.warning(f"Connection closed, reconnect necessary: {reason}")
            self.consumer.reconnect_and_stop()

    def on_connection_open_error(self, _connection, err: Exception):
        self._logger.error(f"Connection open failed: {err}")
        self.consumer.reconnect_and_stop()


class Connection(ConnectionHandler):

    def __init__(self, conn_method: ConnectionMethod, logger: LoggingHandler):
        self._url = conn_method.get_url()
        self._logger = logger

        self._callback: ConnectionCallbackHandler = None
        self._connection: pika.SelectConnection = None
        self._is_conn_closing: bool = False
        self._is_conn_closed: bool = False

    def connect(self):
        self._logger.info(f"Connecting to {self._url}")
        # TODO: Raise exception if callback is not present
        self._connection = pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self._callback.on_connection_open,
            on_open_error_callback=self._callback.on_connection_open_error,
            on_close_callback=self._callback.on_connection_closed,
        )

    def close_connection(self):
        if self._is_conn_closing or self._is_conn_closed:
            self._logger.info("Connection is closing or already closed")
        else:
            self._logger.info("Closing connection")
            self._connection.close()

    def reconnect(self):
        self.should_reconnect = False

    def set_callback(self, callback: ConnectionCallbackHandler):
        self._callback = callback

    def is_closing(self):
        return self._is_conn_closing

    def get_connection(self) -> pika.SelectConnection:
        return self._connection


class ChannelCallback(ChannelCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the channel
        self.consumer.channel.set_callback(self)

    def on_channel_open(self, channel: PikaChannel):
        self._logger.info("Channel opened")
        self.consumer.channel.add_on_channel_close_callback()
        self.consumer.exchange.setup_exchange(channel)

    def on_channel_closed(self, _channel, reason: str):
        self._logger.warning(f"Channel was closed: {reason}")
        self.consumer.consuming = False
        self.consumer.connection.close_connection()


class Channel(ChannelHandler):

    def __init__(self, logger: LoggingHandler):
        self._logger = logger

        self._channel: PikaChannel = None
        self._callback: ChannelCallbackHandler = None

    def open_channel(self, conn: pika.SelectConnection):
        # TODO: Raise exception if callback is not present
        self._logger.info("Creating a new channel")
        self._channel = conn.channel(on_open_callback=self._callback.on_channel_open)

    def close_channel(self):
        self._logger.info("Closing the channel")
        self._channel.close()

    def add_on_channel_close_callback(self):
        self._logger.info("Adding channel close callback")
        self._channel.add_on_close_callback(self._callback.on_channel_closed)

    def set_empty_channel(self):
        self._channel = None

    def set_callback(self, callback: ChannelCallbackHandler):
        self._callback = callback

    def get_channel(self) -> PikaChannel:
        return self._channel


class ExchangeCallback(ExchangeCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the exchange
        self.consumer.exchange.set_callback(self)

    def on_exchange_declare(self, _frame):
        self._logger.info("Exchange declared")
        ch = self.consumer.channel.get_channel()
        self.consumer.queue.setup_queue(ch)


class Exchange(ExchangeHandler):

    def __init__(self, name: str, ex_type: str, logger: LoggingHandler):
        self.name = name
        self._ex_type = ex_type
        self._logger = logger

        self._callback: ExchangeCallbackHandler = None

    def setup_exchange(self, channel: PikaChannel):
        # TODO: Raise exception if callback is not present
        self._logger.info(f"Declaring exchange: {self.name}")
        channel.exchange_declare(
            exchange=self.name,
            exchange_type=self._ex_type,
            callback=self._callback.on_exchange_declare,
        )

    def set_callback(self, callback: ExchangeCallbackHandler):
        self._callback = callback


class QueueCallback(QueueCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the queue
        self.consumer.queue.set_callback(self)

    def on_queue_declare(self, _frame):
        self.consumer.queue.bind_queue(
            self.consumer.exchange, self.consumer.channel.get_channel()
        )

    def on_queue_bind(self, _frame):
        self._logger.info(f"Queue bound: {self.consumer.queue.name}")
        self.consumer.queue.set_qos(self.consumer.channel.get_channel())

    def on_qos_ok(self, _frame):
        self.consumer.start_consuming()


class Queue(QueueHandler):

    def __init__(
        self, name: str, routing_key: str, prefetch_count: int, logger: LoggingHandler
    ):
        self.name = name
        self._routing_key = routing_key
        self._prefetch_count = prefetch_count
        self._logger = logger

        self._callback: QueueCallbackHandler = None

    def setup_queue(self, channel: PikaChannel):
        # TODO: Raise exception if callback is not present
        self._logger.info(f"Declaring queue {self.name}")
        channel.queue_declare(queue=self.name, callback=self._callback.on_queue_declare)

    def bind_queue(self, exchange: ExchangeHandler, channel: PikaChannel):
        self._logger.info(
            f"Binding {exchange.name} to {self.name} with {self._routing_key}"
        )
        channel.queue_bind(
            self.name,
            exchange.name,
            routing_key=self._routing_key,
            callback=self._callback.on_queue_bind,
        )

    def set_qos(self, channel: PikaChannel):
        self._logger.info(f"QOS set to: {self._prefetch_count}")
        channel.basic_qos(
            prefetch_count=self._prefetch_count, callback=self._callback.on_qos_ok
        )

    def set_callback(self, callback: QueueCallbackHandler):
        self._callback = callback


class ConsumerCallback(ConsumerCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the consumer
        self.consumer.set_callback(self)

    def on_consumer_cancelled(self, method_frame):
        self._logger.info(
            f"Consumer was cancelled remotely, shutting down: {method_frame}"
        )
        ch = self.consumer.channel.get_channel()
        if ch:
            ch.close()

    def on_cancel_ok(self, *args, **kwargs):
        self.consumer.consuming = False
        self._logger.info(
            f"Acknowledged the cancellation of the consumer: {self.consumer.consumer_tag}"
        )
        self.consumer.channel.close_channel()


class Consumer(ConsumerHandler):

    def __init__(
        self,
        connection: ConnectionHandler,
        channel: ChannelHandler,
        exchange: ExchangeHandler,
        queue: QueueHandler,
        msg_callback: MessageHandler,
        logger: LoggingHandler,
    ):
        self.connection = connection
        self.channel = channel
        self.exchange = exchange
        self.queue = queue
        self._logger = logger
        self._msg_callback = msg_callback

        self._channel: PikaChannel = None
        self._connection: pika.SelectConnection = None
        self._callback: ConsumerCallbackHandler = None
        self.consumer_tag: str = None

    def run(self):
        self.connection.connect()
        self._connection = self.connection.get_connection()
        self.start_ioloop()

    def stop(self):
        if not self.connection.is_closing:
            self.connection.is_closing = True
            self._logger.info("Stopping")
            if self.consuming:
                self.stop_consuming()
                self.start_ioloop()
            else:
                self.stop_ioloop()
            self._logger.info("Stopped")

    def start_ioloop(self):
        self._connection.ioloop.start()

    def stop_ioloop(self):
        self._connection.ioloop.stop()

    def start_consuming(self):
        # TODO: Raise exception if callback is not present
        self._logger.info("Issuing consumer related RPC commands")
        self._channel = self.channel.get_channel()
        self.add_on_cancel_callback()
        self.consumer_tag = self._channel.basic_consume(
            queue=self.queue.name, callback=self._msg_callback.on_message, auto_ack=True
        )
        self.was_consuming = True
        self.consuming = True

    def stop_consuming(self):
        # TODO: Raise exception if callback is not present
        if self._channel:
            self._logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self._consumer_tag, self._callback.on_cancel_ok)

    def reconnect_and_stop(self):
        self.connection.reconnect()
        self.stop()

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self._logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._callback.on_consumer_cancelled)

    def set_callback(self, callback):
        self._callback = callback
