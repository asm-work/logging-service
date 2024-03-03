"""RabbitMQ message handler: Concrete implementations"""
import pika
from .base_handler import BaseMessageHandler

class Client(BaseMessageHandler):
    
    def __init__(self, url) -> None:
        self.url = url
    
    def create(self):
        parms = pika.URLParameters(self.url)
        connection = pika.BlockingConnection(parms)
        return connection.channel()