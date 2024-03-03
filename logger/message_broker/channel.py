"""
Implementing the message queue functionality using the handler
"""
from message_broker.handlers import rabbitmq


def create_channel(url):
    handler = rabbitmq.Client(url)
    return handler.create()