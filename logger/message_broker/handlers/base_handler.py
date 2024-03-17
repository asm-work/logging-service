"""
Base handlers
"""

from abc import ABC, abstractmethod

from utils.logger import LoggingHandler


class BaseMessageHandler(ABC):
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def create(self, host: str):
        pass


class ConnectionMethod(ABC):

    @abstractmethod
    def __init__(self, config):
        """receives the configuration as parameter"""

    @abstractmethod
    def get_url(self) -> str:
        """Return the URL as a string
        independent of the connection parameters
        """
        pass


class ConnectionHandler(ABC):

    should_reconnect: bool

    @abstractmethod
    def connect(self):
        """Function to create a message queue connection"""
        pass

    @abstractmethod
    def close_connection(self):
        """Function to close the MQ connection"""
        pass

    @abstractmethod
    def reconnect(self):
        """Will be invoked if the connection cant be opened or closed"""
        pass

    @abstractmethod
    def set_callback(self, callback):
        """This function is used to explicitly set the callback for the connection"""
        pass

    @abstractmethod
    def is_closing(self) -> bool:
        """Returns true if the connection is closing"""
        pass

    @abstractmethod
    def get_connection(self):
        """Returns the MQ connection object"""
        pass


class ChannelHandler(ABC):

    @abstractmethod
    def open_channel(self, connection):
        """Open a new channel (If MQ required a channel)"""
        pass

    @abstractmethod
    def close_channel(self):
        """Invoked when MQ unexpectedly closes the channel"""
        pass

    @abstractmethod
    def set_callback(self, callback):
        """This function is used to explicitly set the callback for the channel"""
        pass

    @abstractmethod
    def set_empty_channel(self):
        """Set the channel to None"""
        pass

    @abstractmethod
    def get_channel(self):
        """Return the MQ channel object"""
        pass


class ExchangeHandler(ABC):
    """Only if MQ supports exchange
    Supported MQs
    - RabbitMQ
    """

    name: str

    @abstractmethod
    def setup_exchange(self, channel):
        """Will declare an exchange for creating queue for certain MQs"""
        pass

    @abstractmethod
    def set_callback(self, callback):
        """This function is used to explicitly set the callback for the exchange"""
        pass


class QueueHandler(ABC):

    name: str

    @abstractmethod
    def setup_queue(self, channel):
        """Declare a queue for communication"""
        pass

    @abstractmethod
    def bind_queue(self, *args, **kwargs):
        """To bind the queue"""
        pass

    def set_qos(self):
        """To set the QoS for MQ
        Not mandator for all MQs
        """
        pass

    @abstractmethod
    def set_callback(self, callback):
        """This function is used to explicitly set the callback for the queue"""
        pass


class ConsumerHandler(ABC):

    connection: ConnectionHandler
    channel: ChannelHandler
    exchange: ExchangeHandler
    queue: QueueHandler

    consuming: bool
    was_consuming: bool
    consumer_tag: str

    @abstractmethod
    def run(self):
        """Entry point for starting the consumer"""
        pass

    @abstractmethod
    def stop(self):
        """Shutdown"""
        pass

    @abstractmethod
    def start_consuming(self):
        """Setup the consumer
        Cancel the consumer whichever is running
        """
        pass

    @abstractmethod
    def stop_consuming(self):
        """Cancel the consuming"""
        pass

    def reconnect_and_stop(self):
        """Reconnect the connection and stop the consumer"""
        pass

    @abstractmethod
    def set_callback(self, callback):
        """This function is used to explicitly set the callback for the consumer"""
        pass

    @abstractmethod
    def is_closing(self) -> bool:
        """Returns true if the consumer is closing"""
        pass

    def start_ioloop(self) -> bool:
        """Start the io loop (Method is optional)"""
        pass

    def stop_ioloop(self) -> bool:
        """Stop the io loop (Method is optional)"""
        pass


class ConnectionCallbackHandler(ABC):

    @abstractmethod
    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        """The callback for the connection is set from this method"""
        pass

    @abstractmethod
    def on_connection_open(self, *args, **kwargs):
        """Called when connection is opened"""
        pass

    @abstractmethod
    def on_connection_closed(self, *args, **kwargs):
        """Called when connection is closed"""
        pass

    @abstractmethod
    def on_connection_open_error(self, *args, **kwargs):
        """Called when there is error while opening the connection"""
        pass


class ChannelCallbackHandler(ABC):

    @abstractmethod
    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        """The callback for the channel is set from this method"""
        pass

    @abstractmethod
    def on_channel_open(self, *args, **kwargs):
        """Called when channel is opened"""
        pass

    @abstractmethod
    def on_channel_closed(self, *args, **kwargs):
        """Called when channel is closed"""
        pass


class ExchangeCallbackHandler(ABC):

    @abstractmethod
    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        """The callback for the exchange is set from this method"""
        pass

    @abstractmethod
    def on_exchange_declare(self, *args, **kwargs):
        """Called when an exchange is setup correctly and declared ok"""
        pass


class QueueCallbackHandler(ABC):

    @abstractmethod
    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        """The callback for the queue is set from this method"""
        pass

    @abstractmethod
    def on_queue_declare(self, *args, **kwargs):
        """Called when a queue is setup correctly and declared ok"""
        pass

    @abstractmethod
    def on_queue_bind(self, *args, **kwargs):
        """Called when a queue bind is ok"""
        pass

    @abstractmethod
    def on_queue_bind(self, *args, **kwargs):
        """Called when a queue bind is ok"""
        pass

    def on_qos_ok(self, *args, **kwargs):
        """Called when a QoS is set, Not mandatory"""
        pass


class ConsumerCallbackHandler(ABC):

    @abstractmethod
    def __init__(self, consumer: ConsumerHandler, logger: LoggingHandler):
        """The callback for the consumer is set from this method"""
        pass

    @abstractmethod
    def on_consumer_cancelled(self, *args, **kwargs):
        """Called when consumer is cancelled"""
        pass

    def on_cancel_ok(self, *args, **kwargs):
        """On cancel complete (Optional)"""
        pass
