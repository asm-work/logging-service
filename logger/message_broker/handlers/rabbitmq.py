"""RabbitMQ message handler: Concrete implementations"""

import os

import pika
from dotmap import DotMap
from message.handlers.base_handler import MessageHandler
from message_broker.handlers.base_handler import (
    ChannelCallbackHandler,
    ChannelHandler,
    ConnectionCallbackHandler,
    ConnectionHandler,
    ConnectionMethod,
    ConnectionMethodFactory,
    ConsumerCallbackHandler,
    ConsumerHandler,
    ExchangeCallbackHandler,
    ExchangeHandler,
    QueueCallbackHandler,
    QueueHandler,
)
from pika.channel import Channel as PikaChannel
from utils.constants import Config, GenericConstants
from utils.exceptions import (
    EmptyCallbackErr,
    EmptyChannelErr,
    EmptyExchangeErr,
    EmptyQueueErr,
    InvalidConfigErr,
    NotConnectedErr,
)
from utils.logger import LoggingHandler


class URLMethod(ConnectionMethod):

    def __init__(self, config: DotMap):
        self.config = config

    def get_url(self) -> str | None:
        if self.config.mq.is_url_conn:
            return os.environ.get(Config.MQ_URL.value)
        else:
            raise InvalidConfigErr("config.mq.is_url_conn", self.config.mq.is_url_conn)


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


class URLBasedConnection(ConnectionMethodFactory):

    def get_connection_method(self, config: DotMap) -> URLMethod:
        return URLMethod(config)


class ParameterBasedConnection(ConnectionMethodFactory):

    def get_connection_method(self, config: DotMap) -> ParameterMethod:
        return ParameterMethod(config)


class ConnectionCallback(ConnectionCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the connection
        self.consumer.connection.set_callback(self)

    def on_connection_open(self, connection: pika.SelectConnection):
        if not connection:
            raise NotConnectedErr(connection)
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.channel)
        self._logger.info("Connection opened")
        self.consumer.channel.open_channel(connection)

    def on_connection_closed(self, _connection, reason: str):
        if not self.consumer.connection:
            raise NotConnectedErr(self.consumer.connection)
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.channel)
        self.consumer.channel.set_empty_channel()
        if self.consumer.connection.is_closing():
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

        self.should_reconnect: bool = False
        self._callback: ConnectionCallbackHandler = None
        self._connection: pika.SelectConnection = None
        self._is_conn_closing: bool = False
        self._is_conn_closed: bool = False

    def connect(self):
        self._logger.info(f"Connecting to {self._url}")
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
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
            if self._connection:
                self._connection.close()
            else:
                raise NotConnectedErr(self._connection)

    def reconnect(self):
        self.should_reconnect = True

    def set_callback(self, callback: ConnectionCallbackHandler):
        self._callback = callback

    def is_closing(self):
        return self._is_conn_closing

    def set_as_closing(self):
        self._is_conn_closing = True

    def get_connection(self) -> pika.SelectConnection:
        return self._connection


class ChannelCallback(ChannelCallbackHandler):

    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        self.consumer = consumer
        self._logger = logger
        # Setting this as the callback for the channel
        self.consumer.channel.set_callback(self)

    def on_channel_open(self, channel: PikaChannel):
        if not channel:
            raise EmptyChannelErr(channel)
        if not self.consumer.exchange:
            raise EmptyExchangeErr(self.consumer.exchange)
        self._logger.info("Channel opened")
        self.consumer.channel.add_on_channel_close_callback()
        self.consumer.exchange.setup_exchange(channel)

    def on_channel_closed(self, _channel, reason: str):
        if not self.consumer.connection:
            raise NotConnectedErr(self.consumer.connection)
        self._logger.warning(f"Channel was closed: {reason}")
        self.consumer.consuming = False
        self.consumer.connection.close_connection()


class Channel(ChannelHandler):

    def __init__(self, logger: LoggingHandler):
        self._logger = logger

        self._channel: PikaChannel = None
        self._callback: ChannelCallbackHandler = None

    def open_channel(self, conn: pika.SelectConnection):
        if not conn:
            raise NotConnectedErr(conn)
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
        self._logger.info("Creating a new channel")
        self._channel = conn.channel(on_open_callback=self._callback.on_channel_open)

    def close_channel(self):
        if not self._channel:
            raise EmptyChannelErr(self._channel)
        self._logger.info("Closing the channel")
        self._channel.close()

    def add_on_channel_close_callback(self):
        if not self._channel:
            raise EmptyChannelErr(self._channel)
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
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.channel)
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
        if not channel:
            raise EmptyChannelErr(channel)
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
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
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.queue)
        if not self.consumer.queue:
            raise EmptyQueueErr(self.consumer.queue)
        self.consumer.queue.bind_queue(
            self.consumer.exchange, self.consumer.channel.get_channel()
        )

    def on_queue_bind(self, _frame):
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.queue)
        if not self.consumer.queue:
            raise EmptyQueueErr(self.consumer.queue)
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
        if not channel:
            raise EmptyChannelErr(channel)
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
        self._logger.info(f"Declaring queue {self.name}")
        channel.queue_declare(queue=self.name, callback=self._callback.on_queue_declare)

    def bind_queue(self, exchange: ExchangeHandler, channel: PikaChannel):
        if not channel:
            raise EmptyChannelErr(channel)
        if not exchange:
            raise EmptyExchangeErr(exchange)
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
        self._logger.info(
            f"Binding Exchange: {exchange.name} to Queue: {self.name} with RoutingKey: {self._routing_key}"
        )
        channel.queue_bind(
            queue=self.name,
            exchange=exchange.name,
            routing_key=self._routing_key,
            callback=self._callback.on_queue_bind,
        )

    def set_qos(self, channel: PikaChannel):
        if not channel:
            raise EmptyChannelErr(channel)
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
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
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.channel)
        self._logger.info(
            f"Consumer was cancelled remotely, shutting down: {method_frame}"
        )
        ch = self.consumer.channel.get_channel()
        if ch:
            ch.close()

    def on_cancel_ok(self, _method_frame):
        if not self.consumer.channel:
            raise EmptyChannelErr(self.consumer.channel)
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
        self.consuming: bool = False
        self.was_consuming: bool = False

    def run(self):
        if not self.connection:
            raise NotConnectedErr(self.connection)
        self.connection.connect()
        self._connection = self.connection.get_connection()
        self.start_ioloop()

    def stop(self):
        if not self.connection.is_closing():
            self.connection.set_as_closing()
            self._logger.info("Stopping")
            if self.consuming:
                self.stop_consuming()
                self.start_ioloop()
            else:
                self.stop_ioloop()
            self._logger.info("Stopped")

    def start_ioloop(self):
        if not self._connection:
            raise NotConnectedErr(self._connection)
        self._connection.ioloop.start()

    def stop_ioloop(self):
        if not self._connection:
            raise NotConnectedErr(self._connection)
        self._connection.ioloop.stop()

    def start_consuming(self):
        if not self._msg_callback:
            raise EmptyCallbackErr(self._msg_callback)
        self._logger.info("Issuing consumer related RPC commands")
        self._channel = self.channel.get_channel()
        if not self._channel:
            raise EmptyChannelErr(self._channel)
        self.add_on_cancel_callback()
        self.consumer_tag = self._channel.basic_consume(
            queue=self.queue.name,
            on_message_callback=self._msg_callback.on_message,
            auto_ack=True,
        )
        self.was_consuming = True
        self.consuming = True

    def stop_consuming(self):
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
        if self._channel:
            self._logger.info("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self.consumer_tag, self._callback.on_cancel_ok)

    def reconnect_and_stop(self):
        if not self.connection:
            raise NotConnectedErr(self.connection)
        self.connection.reconnect()
        self.stop()

    def add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        if not self._channel:
            raise EmptyChannelErr(self._channel)
        if not self._callback:
            raise EmptyCallbackErr(self._callback)
        self._logger.info("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._callback.on_consumer_cancelled)

    def set_callback(self, callback):
        self._callback = callback


def create_connection_method(conn_method: GenericConstants) -> ConnectionMethodFactory:
    """Return the connection method instance"""
    factories = {
        GenericConstants.URL_CONN_METHOD: URLBasedConnection(),
        GenericConstants.PARAM_CONN_METHOD: ParameterBasedConnection(),
    }
    return factories[conn_method]
