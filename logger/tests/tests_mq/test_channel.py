from unittest.mock import Mock

import pytest
from __mock__ import pika
from base_test import BaseTest
from message_broker.connect import Connection


class TestConnection(BaseTest):
    # Checking the channel creation
    # TODO: Complete all the test
    @pytest.mark.unit
    def test_channel(self, monkeypatch):
        mocked_pika = Mock()
        mocked_pika.URLParameters.return_value = "TEST_PARAMS"
        monkeypatch.setattr("message_broker.connect.rabbitmq.pika", mocked_pika)

        self.load_envs()
        conn = Connection(self.config_data)
        conn.create_channel()
        mocked_pika.URLParameters.assert_called_once_with(self.mq_url)

    @pytest.mark.unit
    def test_channel_create_connection(self, monkeypatch):
        mocked_pika = Mock()
        mocked_pika.URLParameters.return_value = "TEST_PARAMS"
        mocked_pika.BlockingConnection.return_value = pika.Connection()
        monkeypatch.setattr("message_broker.connect.rabbitmq.pika", mocked_pika)

        self.load_envs()
        conn = Connection(self.config_data)
        conn.create_channel()
        mocked_pika.BlockingConnection.assert_called_once_with("TEST_PARAMS")
