from unittest.mock import Mock

import pytest
from __mock__ import pika
from base_test import BaseTest
from message_broker.connect import Connection


class TestListener(BaseTest):

    def mocked_handler(self):
        pass

    @pytest.mark.unit
    def test_listener_queue_declared(self, monkeypatch):
        channel = pika.Channel()
        channel.queue_declare = Mock()

        self.load_envs()
        conn = Connection(self.config_data)
        conn.subscribe(channel=channel, handler=self.mocked_handler)

        channel.queue_declare.assert_called_once_with(
            queue=self.config_data.mq.unit_test.queue,
            durable=self.config_data.mq.unit_test.durable,
        )

    @pytest.mark.unit
    def test_listener_queue_bind(self, monkeypatch):
        channel = pika.Channel()
        channel.queue_bind = Mock()

        self.load_envs()
        conn = Connection(self.config_data)
        conn.subscribe(channel=channel, handler=self.mocked_handler)

        channel.queue_bind.assert_called_once_with(
            queue=self.config_data.mq.unit_test.queue,
            exchange=self.config_data.mq.unit_test.exchange,
        )

    @pytest.mark.unit
    def test_listener_qos(self, monkeypatch):
        channel = pika.Channel()
        channel.basic_qos = Mock()

        self.load_envs()
        conn = Connection(self.config_data)
        conn.subscribe(channel=channel, handler=self.mocked_handler)

        channel.basic_qos.assert_called_once_with(
            prefetch_count=self.config_data.mq.unit_test.prefetch.count
        )

    @pytest.mark.unit
    def test_listener_consume(self, monkeypatch):
        channel = pika.Channel()
        channel.basic_consume = Mock()

        self.load_envs()
        conn = Connection(self.config_data)
        conn.subscribe(channel=channel, handler=self.mocked_handler)

        channel.basic_consume.assert_called_once_with(
            queue=self.config_data.mq.unit_test.queue,
            on_message_callback=self.mocked_handler,
        )
