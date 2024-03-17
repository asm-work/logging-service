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
        body = kwargs.get("body", "")
        self._logger.info(f"Received the message: MSG: {body} @ {time.time}")
