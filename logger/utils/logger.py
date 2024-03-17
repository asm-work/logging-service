import time
from abc import ABC, abstractmethod


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


class StdOutLogger(LoggingHandler):

    def __init__(self):
        """This logger is only for development"""
        pass

    def debug(self, msg):
        print(f"Level: DEBUG, Message: {msg}, Time: {time.time}")

    def info(self, msg):
        print(f"Level: INFO, Message: {msg}, Time: {time.time}")

    def error(self, msg):
        print(f"Level: ERROR, Message: {msg}, Time: {time.time}")

    def warning(self, msg):
        print(f"Level: WARNING, Message: {msg}, Time: {time.time}")


# TODO: Need to implement the file based logging
