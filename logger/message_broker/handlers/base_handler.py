"""
Base handers
"""
from abc import ABC, abstractclassmethod


class BaseMessageHandler(ABC):
    @abstractclassmethod
    def __init__(self) -> None:
        pass

    @abstractclassmethod
    def create(self, host: str):
        pass
