"""
Handling the messages
"""

import time

from message.handlers.base_handler import MessageHandler
from utils.logger import LoggingHandler


class Message(MessageHandler):

    def __init__(self, logger: LoggingHandler):
        self._logger = logger

    def on_message(self, *args, **kwargs):
        # Implement the functionality
        self.message = kwargs.get("body", "")
        self._logger.info(f"Received the message: MSG: {self.message} @ {time.time}")

    def validate(self) -> bool:
        """
        Check whether the message is valid or not and return the status
        """
        if self.message:
            return True
        return False

    def parse_message(self) -> None:
        """
        Parse the incoming message
        """
        return
