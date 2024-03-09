"""
Base handers
"""
from abc import ABC, abstractmethod


class BaseMessageHandler(ABC):
    @abstractmethod
    def __init__(self) -> None:
        pass

    @abstractmethod
    def create(self, host: str):
        pass
