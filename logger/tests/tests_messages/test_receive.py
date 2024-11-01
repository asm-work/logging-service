from enum import Enum
from unittest.mock import Mock

import pytest
from __mock__ import logger
from base_test import BaseTest
from message.write import Message
from pytest import MonkeyPatch


class MonkeyPatchModules(Enum):
    LOGGER = "utils.logger.BuiltinLogger"


class MessageBaseTest(BaseTest):

    def mock_logger(self, monkeypatch: MonkeyPatch):
        self.mocked_logger = Mock()
        self.mocked_logger.BuiltinLogger.return_value = logger.BuiltinLogger()
        monkeypatch.setattr(MonkeyPatchModules.LOGGER.value, self.mocked_logger)


class TestMessage(MessageBaseTest):

    @pytest.mark.unit
    def test_validate_message(self, monkeypatch: MonkeyPatch):
        self.mock_logger(monkeypatch)
        message = Message(logger=self.mocked_logger)
        message.on_message(body="test message")
        message.validate()

    @pytest.mark.unit
    def test_parse_message(self, monkeypatch: MonkeyPatch):
        self.mock_logger(monkeypatch)
        message = Message(logger=self.mocked_logger)
        message.on_message(body="test message")
        message.parse_message()
