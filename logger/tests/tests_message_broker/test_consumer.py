"""Unit test for consumer functionality
"""

from enum import Enum, auto
from unittest.mock import Mock

import pytest
from __mock__ import callbacks, logger, pika
from base_test import BaseTest
from message_broker.handlers import rabbitmq as mq
from pytest import MonkeyPatch
from utils.exceptions import (
    EmptyCallbackErr,
    EmptyChannelErr,
    EmptyExchangeErr,
    InvalidConfigErr,
    NotConnectedErr,
)


class MokeModules(Enum):
    ENV = auto()
    PIKA = auto()
    LOGGER = auto()
    CONNECTION = auto()
    CHANNEL = auto()
    EXCHANGE = auto()
    QUEUE = auto()
    CONSUMER = auto()


class ConsumerBaseTest(BaseTest):

    def set_pika(self, monkeypatch: MonkeyPatch):
        self.mocked_pika = Mock()
        self.mocked_pika.SelectConnection.return_value = pika.SelectConnection()
        monkeypatch.setattr("message_broker.handlers.rabbitmq.pika", self.mocked_pika)

    def set_logger(self, monkeypatch: MonkeyPatch):
        self.mocked_logger = Mock()
        self.mocked_logger.BuiltinLogger.return_value = logger.BuiltinLogger()
        monkeypatch.setattr("utils.logger.BuiltinLogger", self.mocked_logger)

    def get_conn_obj(
        self, monkeypatch: MonkeyPatch, moke_except: list[MokeModules] = []
    ) -> mq.Connection:
        if MokeModules.ENV not in moke_except:
            self.load_envs()
        if MokeModules.PIKA not in moke_except:
            self.set_pika(monkeypatch)
        if MokeModules.LOGGER not in moke_except:
            self.set_logger(monkeypatch)
        self.mocked_conn_callback = Mock()

        self.mocked_conn_callback.ConnectionCallback.return_value = (
            callbacks.ConnectionCallback()
        )
        monkeypatch.setattr(
            "message_broker.handlers.rabbitmq.ConnectionCallback",
            self.mocked_conn_callback,
        )

        url_method = mq.URLMethod(config=self.config_data)
        conn = mq.Connection(conn_method=url_method, logger=self.mocked_logger)
        return conn

    def get_chan_obj(
        self, monkeypatch: MonkeyPatch, moke_except: list[MokeModules] = []
    ) -> mq.Channel:
        if MokeModules.ENV not in moke_except:
            self.load_envs()
        if MokeModules.PIKA not in moke_except:
            self.set_pika(monkeypatch)
        if MokeModules.LOGGER not in moke_except:
            self.set_logger(monkeypatch)
        self.mocked_chan_callback = Mock()

        self.mocked_chan_callback.ChannelCallback.return_value = (
            callbacks.ChannelCallback()
        )
        monkeypatch.setattr(
            "message_broker.handlers.rabbitmq.ChannelCallback",
            self.mocked_chan_callback,
        )
        chan = mq.Channel(logger=self.mocked_logger)
        return chan

    def get_exch_obj(
        self, monkeypatch: MonkeyPatch, moke_except: list[MokeModules] = []
    ) -> mq.Exchange:
        if MokeModules.ENV not in moke_except:
            self.load_envs()
        if MokeModules.PIKA not in moke_except:
            self.set_pika(monkeypatch)
        if MokeModules.LOGGER not in moke_except:
            self.set_logger(monkeypatch)
        self.mocked_exch_callback = Mock()

        self.mocked_exch_callback.ExchangeCallback.return_value = (
            callbacks.ExchangeCallback()
        )
        monkeypatch.setattr(
            "message_broker.handlers.rabbitmq.ExchangeCallback",
            self.mocked_exch_callback,
        )
        exch = mq.Exchange(
            name=self.config_data.mq.exchange.name,
            ex_type=self.config_data.mq.exchange.type,
            logger=self.mocked_logger,
        )
        return exch

    def get_queue_obj(
        self, monkeypatch: MonkeyPatch, moke_except: list[MokeModules] = []
    ) -> mq.Queue:
        if MokeModules.ENV not in moke_except:
            self.load_envs()
        if MokeModules.PIKA not in moke_except:
            self.set_pika(monkeypatch)
        if MokeModules.LOGGER not in moke_except:
            self.set_logger(monkeypatch)
        self.mocked_queue_callback = Mock()

        self.mocked_queue_callback.QueueCallback.return_value = (
            callbacks.QueueCallback()
        )
        monkeypatch.setattr(
            "message_broker.handlers.rabbitmq.QueueCallback",
            self.mocked_queue_callback,
        )
        queue = mq.Queue(
            name=self.config_data.mq.queue,
            routing_key=self.config_data.mq.routing_key,
            prefetch_count=self.config_data.mq.prefetch.count,
            logger=self.mocked_logger,
        )
        return queue

    def get_consumer_obj(self, monkeypatch: MonkeyPatch) -> mq.Consumer:
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        exch = self.get_exch_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        exch.set_callback(self.mocked_exch_callback)
        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)

        self.mocked_consumer_callback = Mock()
        self.mocked_consumer_callback.ConsumerCallback.return_value = (
            callbacks.ConsumerCallback()
        )
        monkeypatch.setattr(
            "message_broker.handlers.rabbitmq.ConsumerCallback",
            self.mocked_consumer_callback,
        )

        self.mocked_message_callback = Mock()
        self.mocked_message_callback.Message.return_value = callbacks.MessageCallback()
        monkeypatch.setattr(
            "message.write.Message",
            self.mocked_message_callback,
        )

        consumer = mq.Consumer(
            connection=conn,
            channel=chan,
            exchange=exch,
            queue=queue,
            msg_callback=self.mocked_message_callback,
            logger=self.mocked_logger,
        )
        return consumer


class TestConnection(ConsumerBaseTest):

    @pytest.mark.unit
    def test_url_method(self):
        self.load_envs()
        url_method = mq.URLMethod(config=self.config_data)
        assert url_method.get_url() == self.mq_url

    @pytest.mark.unit
    def test_url_method_invalid_config(self):
        self.load_envs()
        self.config_data.mq.is_url_conn = False
        url_method = mq.URLMethod(config=self.config_data)
        with pytest.raises(InvalidConfigErr):
            url_method.get_url()

    @pytest.mark.unit
    def test_parameter_method(self):
        self.load_envs()
        url_method = mq.ParameterMethod(config=self.config_data)
        assert url_method.get_url() == self.mq_url

    @pytest.mark.unit
    def test_connect_with_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()
        assert isinstance(conn._connection, pika.SelectConnection)

    @pytest.mark.unit
    def test_connect_without_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        with pytest.raises(EmptyCallbackErr):
            assert conn.connect()

    @pytest.mark.unit
    def test_close_connection(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()
        conn.close_connection()

    @pytest.mark.unit
    def test_close_connection_without_connect(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        with pytest.raises(NotConnectedErr):
            conn.close_connection()

    @pytest.mark.unit
    def test_reconnect(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()
        conn.reconnect()
        assert conn.should_reconnect is True

    @pytest.mark.unit
    def test_connection_is_closing(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()
        assert conn.is_closing() is False

    @pytest.mark.unit
    def test_connection_set_as_closing(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()
        conn.set_as_closing()
        assert conn.is_closing() is True

    @pytest.mark.unit
    def test_get_connection(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()
        assert conn.get_connection() == self.mocked_pika.SelectConnection()

    @pytest.mark.unit
    def test_get_connection_without_connect(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        assert conn.get_connection() is None


class TestChannel(ConsumerBaseTest):

    @pytest.mark.unit
    def test_open_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)
        assert isinstance(chan._channel, pika.Channel)

    @pytest.mark.unit
    def test_open_channel_without_connection(self, monkeypatch: MonkeyPatch):
        chan = self.get_chan_obj(monkeypatch)
        with pytest.raises(NotConnectedErr):
            chan.open_channel(conn=None)

    @pytest.mark.unit
    def test_open_channel_without_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        with pytest.raises(EmptyCallbackErr):
            chan.open_channel(conn=conn._connection)

    @pytest.mark.unit
    def test_close_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)
        chan.close_channel()

        assert isinstance(chan._channel, pika.Channel)

    @pytest.mark.unit
    def test_close_channel_without_opening(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        with pytest.raises(EmptyChannelErr):
            chan.close_channel()

    @pytest.mark.unit
    def test_add_on_channel_close_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)
        chan.add_on_channel_close_callback()

    @pytest.mark.unit
    def test_add_on_channel_close_callback_empty_channel(
        self, monkeypatch: MonkeyPatch
    ):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        with pytest.raises(EmptyChannelErr):
            chan.add_on_channel_close_callback()

    @pytest.mark.unit
    def test_set_empty_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)
        chan.set_empty_channel()

        assert chan._channel is None

    @pytest.mark.unit
    def test_get_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)
        assert isinstance(chan.get_channel(), pika.Channel)


class TestExchange(ConsumerBaseTest):

    @pytest.mark.unit
    def test_setup_exchange(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        exch = self.get_exch_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        exch.set_callback(self.mocked_exch_callback)
        exch.setup_exchange(chan._channel)
        assert isinstance(chan.get_channel(), pika.Channel)
        assert exch.name == self.config_data.mq.exchange.name

    @pytest.mark.unit
    def test_setup_exchange_without_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        exch = self.get_exch_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        with pytest.raises(EmptyCallbackErr):
            exch.setup_exchange(chan._channel)

    @pytest.mark.unit
    def test_setup_exchange_without_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)

        exch = self.get_exch_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        with pytest.raises(EmptyChannelErr):
            exch.setup_exchange(chan._channel)


class TestQueue(ConsumerBaseTest):

    @pytest.mark.unit
    def test_setup_queue(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)
        queue.setup_queue(channel=chan._channel)

        assert isinstance(chan.get_channel(), pika.Channel)
        assert queue.name == self.config_data.mq.queue

    @pytest.mark.unit
    def test_setup_queue_without_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        with pytest.raises(EmptyCallbackErr):
            queue.setup_queue(channel=chan._channel)

    @pytest.mark.unit
    def test_setup_queue_without_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)
        with pytest.raises(EmptyChannelErr):
            queue.setup_queue(channel=chan._channel)

    @pytest.mark.unit
    def test_setup_bind_queue(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        exch = self.get_exch_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        exch.set_callback(self.mocked_exch_callback)
        exch.setup_exchange(channel=chan._channel)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)
        queue.bind_queue(exchange=exch, channel=chan._channel)

        assert isinstance(chan.get_channel(), pika.Channel)

    @pytest.mark.unit
    def test_setup_bind_queue_without_exchange(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)
        with pytest.raises(EmptyExchangeErr):
            queue.bind_queue(exchange=None, channel=chan._channel)

    @pytest.mark.unit
    def test_setup_bind_queue_without_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        exch = self.get_exch_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        exch.set_callback(self.mocked_exch_callback)
        exch.setup_exchange(channel=chan._channel)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        with pytest.raises(EmptyCallbackErr):
            queue.bind_queue(exchange=exch, channel=chan._channel)

    @pytest.mark.unit
    def test_set_qos(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)
        queue.set_qos(channel=chan._channel)

    @pytest.mark.unit
    def test_set_qos_without_channel(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        queue.set_callback(self.mocked_queue_callback)
        with pytest.raises(EmptyChannelErr):
            queue.set_qos(channel=chan._channel)

    @pytest.mark.unit
    def test_set_qos_without_callback(self, monkeypatch: MonkeyPatch):
        conn = self.get_conn_obj(monkeypatch)
        conn.set_callback(self.mocked_conn_callback)
        conn.connect()

        chan = self.get_chan_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        chan.set_callback(self.mocked_chan_callback)
        chan.open_channel(conn=conn._connection)

        queue = self.get_queue_obj(
            monkeypatch,
            moke_except=[MokeModules.ENV, MokeModules.PIKA, MokeModules.LOGGER],
        )
        with pytest.raises(EmptyCallbackErr):
            queue.set_qos(channel=chan._channel)


class TestConsumer(ConsumerBaseTest):

    @pytest.mark.unit
    def test_run(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.run()
        assert consumer.connection is not None

    @pytest.mark.unit
    def test_run_without_connection(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.connection = None
        with pytest.raises(NotConnectedErr):
            consumer.run()

    @pytest.mark.unit
    def test_stop(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.run()
        consumer.stop()
        assert consumer.connection.is_closing

    @pytest.mark.unit
    def test_stop_without_run(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        with pytest.raises(NotConnectedErr):
            consumer.stop()

    @pytest.mark.unit
    def test_start_io_loop(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.run()
        consumer.start_ioloop()
        assert consumer.connection is not None

    @pytest.mark.unit
    def test_start_io_loop_without_connection(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        with pytest.raises(NotConnectedErr):
            consumer.start_ioloop()

    @pytest.mark.unit
    def test_stop_io_loop(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.run()
        consumer.stop_ioloop()

    @pytest.mark.unit
    def test_stop_io_loop_without_connection(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        with pytest.raises(NotConnectedErr):
            consumer.stop_ioloop()

    @pytest.mark.unit
    def test_start_consuming(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.set_callback(self.mocked_consumer_callback)
        consumer.run()
        consumer.channel.open_channel(consumer.connection.get_connection())
        consumer.start_consuming()

    @pytest.mark.unit
    def test_start_consuming_without_message_callback(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer._msg_callback = None
        consumer.run()
        consumer.channel.open_channel(consumer.connection.get_connection())
        with pytest.raises(EmptyCallbackErr):
            consumer.start_consuming()

    @pytest.mark.unit
    def test_start_consuming_without_channel_open(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.set_callback(self.mocked_consumer_callback)
        consumer.run()
        with pytest.raises(EmptyChannelErr):
            consumer.start_consuming()

    @pytest.mark.unit
    def test_stop_consuming(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.set_callback(self.mocked_consumer_callback)
        consumer.run()
        consumer.channel.open_channel(consumer.connection.get_connection())
        consumer.start_consuming()
        consumer.stop_consuming()

    @pytest.mark.unit
    def test_stop_consuming_without_callback(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.run()
        consumer.channel.open_channel(consumer.connection.get_connection())
        with pytest.raises(EmptyCallbackErr):
            consumer.stop_consuming()

    @pytest.mark.unit
    def test_stop_consuming_without_channel(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.set_callback(self.mocked_consumer_callback)
        consumer.run()
        consumer.stop_consuming()

    @pytest.mark.unit
    def test_reconnect_and_stop(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.set_callback(self.mocked_consumer_callback)
        consumer.run()
        consumer.reconnect_and_stop()

    @pytest.mark.unit
    def test_add_on_cancel_callback(self, monkeypatch: MonkeyPatch):
        consumer = self.get_consumer_obj(monkeypatch)
        consumer.set_callback(self.mocked_consumer_callback)
        consumer.run()
        consumer.channel.open_channel(consumer.connection.get_connection())
        consumer.start_consuming()
        consumer.add_on_cancel_callback()
