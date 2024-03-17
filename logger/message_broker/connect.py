"""
Implementing the message queue functionality using the handler
"""

import time

from dotmap import DotMap
from message.write import Message
from message_broker.handlers import rabbitmq as mq
from utils.constants import Config
from utils.logger import BuiltinLogger

# class Connection:
#     def __init__(self, config) -> None:
#         self.config = config

#     def create_channel(self):
#         # Create a new channel connection with the given parameters
#         if self.config.mq.is_url_conn:
#             handler = mq.Client(os.environ.get("MQ_URL"))
#             return handler.create()
#         else:
#             handler = mq.Client()
#             host = os.environ.get("MQ_HOST")
#             port = os.environ.get("MQ_PORT")
#             user = os.environ.get("MQ_USER", "guest")
#             password = os.environ.get("MQ_PASS", "guest")
#             handler.generate_url(host=host, port=port, user=user, password=password)
#             return handler.create()

#     def subscribe(self, channel, handler, **kwargs):
#         channel.queue_declare(
#             queue=self.config.mq.unit_test.queue,
#             durable=self.config.mq.unit_test.durable,
#         )
#         channel.queue_bind(
#             queue=self.config.mq.unit_test.queue,
#             exchange=self.config.mq.unit_test.exchange,
#         )
#         channel.basic_qos(prefetch_count=self.config.mq.unit_test.prefetch.count)
#         channel.basic_consume(
#             queue=self.config.mq.unit_test.queue, on_message_callback=handler
#         )


class Connection:

    def __init__(self, config: DotMap):
        self._config = config
        self._reconnect_delay = 0
        self._create_consumer()

    def _create_consumer(self):
        # Setting the logger
        # TODO: use the correct logging service (db based)
        self._logger = BuiltinLogger(
            name=__name__,
            log_format=Config.LOG_FORMAT.value,
            handler=Config.LOG_FILE_HANDLER,
        )
        self._logger.info("Initiating a new connection ...")
        # Creating new connection
        conn = mq.Connection(
            conn_method=mq.URLMethod(self._config), logger=self._logger
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
        # TODO: Set the message callback
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
