from abc import ABC, abstractmethod


class MessageHandler(ABC):

    @abstractmethod
    def on_message(self, *args, **kwargs):
        """The function to execute when a new message delivers"""
        pass
