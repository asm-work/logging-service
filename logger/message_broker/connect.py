"""
Implementing the message queue functionality using the handler
"""

import time

from dotmap import DotMap
from message.write import Message
from message_broker.handlers import rabbitmq as mq
from utils.constants import Config, GenericConstants
from utils.logger import create_logger


class Connection:

    def __init__(self, config: DotMap):
        self._config = config
        self._reconnect_delay = 0
        self._create_consumer()

    def _create_consumer(self):
        # Setting the logger
        # TODO: use the correct logging service (db based)
        log_fac = create_logger(GenericConstants.GENERIC_LOGGER)
        self._logger = log_fac.get_logger(
            name=__name__,
            log_format=Config.LOG_FORMAT.value,
            handler=Config.LOG_FILE_HANDLER,
        )
        self._logger.info("Initiating a new connection ...")
        # Creating new connection
        conn_method_fac = mq.create_connection_method(GenericConstants.URL_CONN_METHOD)
        conn = mq.Connection(
            conn_method=conn_method_fac.get_connection_method(self._config),
            logger=self._logger,
        )
        # Creating new channel
        chan = mq.Channel(logger=self._logger)
        # Creating new exchange
        x = mq.Exchange(
            name=self._config.mq.exchange.name,
            ex_type=self._config.mq.exchange.type,
            logger=self._logger,
        )
        # Creating new queue
        q = mq.Queue(
            name=self._config.mq.queue,
            routing_key=self._config.mq.routing_key,
            prefetch_count=self._config.mq.prefetch.count,
            logger=self._logger,
        )
        # Creating new consumer
        self._consumer = mq.Consumer(
            connection=conn,
            channel=chan,
            exchange=x,
            queue=q,
            msg_callback=Message(logger=self._logger),
            logger=self._logger,
        )
        # Registering Callbacks
        mq.ConnectionCallback(consumer=self._consumer, logger=self._logger)
        mq.ChannelCallback(consumer=self._consumer, logger=self._logger)
        mq.ExchangeCallback(consumer=self._consumer, logger=self._logger)
        mq.QueueCallback(consumer=self._consumer, logger=self._logger)
        mq.ConsumerCallback(consumer=self._consumer, logger=self._logger)

    def run(self):
        while True:
            try:
                self._logger.info("Starting the consumer...")
                self._consumer.run()
            # TODO: add the exact exception
            except KeyboardInterrupt:
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        """Create a new consumer if there is a reconnect request"""
        if self._consumer.connection.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            self._logger.info(f"Reconnecting after {reconnect_delay} seconds")
            time.sleep(reconnect_delay)
            self._create_consumer()

    def _get_reconnect_delay(self):
        # TODO: add a better logic for reconnect delay
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
