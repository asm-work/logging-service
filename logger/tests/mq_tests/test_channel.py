import pytest
import message_broker
from unittest.mock import Mock
from tests.__mock__ import pika


class TestConnection:
    
    @pytest.mark.unit
    def test_channel(self, monkeypatch):
        mocked_pika = Mock()
        mocked_pika.URLParameters.return_value = 'TEST_PARAMS'
        monkeypatch.setattr('message_broker.channel.rabbitmq.pika', mocked_pika)
        message_broker.channel.create_channel('TEST_HOST')
        mocked_pika.URLParameters.assert_called_once_with('TEST_HOST')

    @pytest.mark.unit
    def test_channel_create_connection(self, monkeypatch):
        mocked_pika = Mock()
        mocked_pika.URLParameters.return_value = 'TEST_PARAMS'
        mocked_pika.BlockingConnection.return_value = pika.Connection()
        monkeypatch.setattr('message_broker.channel.rabbitmq.pika', mocked_pika)
        message_broker.channel.create_channel('TEST_HOST')
        mocked_pika.BlockingConnection.assert_called_once_with('TEST_PARAMS')
        
        