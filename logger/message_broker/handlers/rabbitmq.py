"""RabbitMQ message handler: Concrete implementations"""
import os
import pika
from pika.channel import Channel as PikaChannel
from dotmap import DotMap
from message_broker.handlers.base_handler import (
    BaseMessageHandler, ChannelCallbackHandler, ChannelHandler, ConnectionMethod, ConnectionHandler, ConsumerHandler,
    ConnectionCallbackHandler, ExchangeCallbackHandler, ExchangeHandler
)
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

    def get_url(self) -> str:
        if self.config.mq.is_url_conn:
            return os.environ.get(Config.MQ_URL.value)
        # TODO: add exception handler


class ParameterMethod(ConnectionMethod):

    def __init__(self, config: DotMap):
        self.config = config

    def get_url(self) -> str:
        # TODO: Give the provision for other advanced parameters
        host = os.environ.get(Config.MQ_HOST.value,
                              Config.MQ_HOST_DEFAULT.value)
        port = os.environ.get(Config.MQ_PORT.value,
                              Config.MQ_PORT_DEFAULT.value)
        user = os.environ.get(Config.MQ_USER.value,
                              Config.MQ_USER_DEFAULT.value)
        pswd = os.environ.get(Config.MQ_PASS.value,
                              Config.MQ_PASS_DEFAULT.value)
        return f"amqp://{user}:{pswd}@{host}:{port}/"


class ConnectionCallback(ConnectionCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the connection
        self.consumer.connection.set_callback(self)

    def on_connection_open(self, connection: pika.SelectConnection):
        self._logger.info('Connection opened')
        self.consumer.channel.open_channel(connection)

    def on_connection_closed(self, _connection, reason: str):
        self.consumer.channel.set_empty_channel()
        if self.consumer.is_closing():
            self.consumer.stop_ioloop()
        else:
            self._logger.warning(
                f'Connection closed, reconnect necessary: {reason}')
            self.consumer.reconnect_and_stop()

    def on_connection_open_error(self, _connection, err: Exception):
        self._logger.error(f'Connection open failed: {err}')
        self.consumer.reconnect_and_stop()


class Connection(ConnectionHandler):

    def __init__(
        self, conn_method: ConnectionMethod,
        logger: LoggingHandler
    ):
        self._url = conn_method.get_url()
        self._logger = logger

        self._callback: ConnectionCallbackHandler = None
        self._connection: pika.SelectConnection = None
        self._is_conn_closing: bool = False
        self._is_conn_closed: bool = False
        self._should_reconnect: bool = False

    def connect(self):
        self._logger.info(f'Connecting to {self._url}')
        # TODO: Raise exception if callback is not present
        self._connection = pika.SelectConnection(
            parameters=pika.URLParameters(self._url),
            on_open_callback=self._callback.on_connection_open,
            on_open_error_callback=self._callback.on_connection_open_error,
            on_close_callback=self._callback.on_connection_closed)

    def close_connection(self):
        if self._is_conn_closing or self._is_conn_closed:
            self._logger.info('Connection is closing or already closed')
        else:
            self._logger.info('Closing connection')
            self._connection.close()

    def reconnect(self):
        self._should_reconnect = False

    def set_callback(self, callback: ConnectionCallbackHandler):
        self._callback = callback

    def is_closing(self):
        return self._is_conn_closing


class ChannelCallback(ChannelCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the channel
        self.consumer.channel.set_callback(self)

    def on_channel_open(self, channel: PikaChannel):
        self._logger.info('Channel opened')
        self.consumer.channel.add_on_channel_close_callback()
        self.consumer.exchange.setup_exchange(channel)

    def on_channel_closed(self, _channel, reason: str):
        self._logger.warning(f'Channel was closed: {reason}')
        self.consumer.consuming = False
        self.consumer.connection.close_connection()


class Channel(ChannelHandler):

    def __init__(self, logger: LoggingHandler):
        self._logger = logger

        self._channel: PikaChannel = None
        self._callback: ChannelCallbackHandler = None

    def open_channel(self, conn: pika.SelectConnection):
        self._logger.info('Creating a new channel')
        self._channel = conn.channel(
            on_open_callback=self._callback.on_channel_open)

    def add_on_channel_close_callback(self):
        self._logger.info('Adding channel close callback')
        self._channel.add_on_close_callback(self._callback.on_channel_closed)

    def set_callback(self, callback: ChannelCallbackHandler):
        self._callback = callback


class ExchangeCallback(ExchangeCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the exchange
        self.consumer.exchange.set_callback(self)

    def on_exchange_declare(self, _frame):
        self._logger.info('Exchange declared')
        self.consumer.queue.setup_queue()


class Exchange(ExchangeHandler):

    def __init__(self, config: DotMap, logger: LoggingHandler):
        self._config = config
        self._logger = logger

        self._callback: ExchangeCallbackHandler = None

    def setup_exchange(self, channel: PikaChannel):
        self._logger.info(
            f'Declaring exchange: {self._config.mq.exchange.name}')
        channel.exchange_declare(
            exchange=self._config.mq.exchange.name,
            exchange_type=self._config.mq.exchange.type,
            callback=self._callback.on_exchange_declare
        )

    def set_callback(self, callback: ExchangeCallbackHandler):
        self._callback = callback
