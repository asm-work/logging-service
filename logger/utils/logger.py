import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from logging.handlers import RotatingFileHandler

from utils.constants import Config, GenericConstants


class LoggingHandler(ABC):

    @abstractmethod
    def __init__(self, *args, **kwargs):
        """Initialize the logger"""
        pass

    @abstractmethod
    def debug(self, msg):
        pass

    @abstractmethod
    def info(self, msg):
        pass

    @abstractmethod
    def error(self, msg):
        pass

    @abstractmethod
    def warning(self, msg):
        pass


class PrintLogger(LoggingHandler):

    def __init__(self, *args, **kwargs):
        """This logger is only for development"""
        pass

    def debug(self, msg):
        print(f"Level: DEBUG, Message: {msg}, Time: {datetime.today()}")

    def info(self, msg):
        print(f"Level: INFO, Message: {msg}, Time: {datetime.today()}")

    def error(self, msg):
        print(f"Level: ERROR, Message: {msg}, Time: {datetime.today()}")

    def warning(self, msg):
        print(f"Level: WARNING, Message: {msg}, Time: {datetime.today()}")


class BuiltinLogger(LoggingHandler):

    def __init__(
        self, name: str, log_format: str, handler: Config = Config.LOG_STREAM_HANDLER
    ):
        """Builtin logging module
        Args:
            name (str): logger name
            log_format (str): log format style
            handler (Config, optional): The log writing handler can be. [Config.LOG_STREAM_HANDLER | Config.LOG_FILE_HANDLER]
                                        Defaults to Config.LOG_STREAM_HANDLER.
        """
        self._format = log_format
        self._name = name
        self._handler_name = handler

        self._handler: logging.Handler = None
        self._set_logger()

    def _set_logger(self):
        if self._handler_name == Config.LOG_FILE_HANDLER:
            self._handler = RotatingFileHandler(
                filename=os.path.join(
                    Config.LOG_FILE_DIR.value, Config.LOG_FILE_NAME.value
                ),
                maxBytes=1024,
                backupCount=10,
            )
        elif self._handler_name == Config.LOG_STREAM_HANDLER:
            self._handler = logging.StreamHandler()
        else:
            # TODO: Add the specific exception
            raise Exception(f"Invalid log handler {self._handler_name}")
        self._handler.setFormatter(logging.Formatter(self._format))

        self.logger = logging.getLogger(self._name)
        self.logger.addHandler(self._handler)
        self.logger.setLevel(logging.DEBUG)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

    def warning(self, msg):
        self.logger.warning(msg)


# TODO: Need to implement the DB based logging


class LoggerFactory(ABC):

    def get_logger(self) -> LoggingHandler:
        """Returns the logging handler instance"""
        pass


class ConsoleLogger(LoggerFactory):

    def get_logger(self, *args, **kwargs) -> PrintLogger:
        return PrintLogger(*args, **kwargs)


class GenericLogger(LoggerFactory):

    def get_logger(self, *args, **kwargs) -> BuiltinLogger:
        return BuiltinLogger(*args, **kwargs)


def create_logger(logger: GenericConstants) -> LoggerFactory:
    factories = {
        GenericConstants.CONSOLE_LOGGER: ConsoleLogger(),
        GenericConstants.GENERIC_LOGGER: GenericLogger(),
    }

    return factories[logger]
