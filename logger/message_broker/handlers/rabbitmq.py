"""RabbitMQ message handler: Concrete implementations"""
import pika
from .base_handler import BaseMessageHandler


class Client(BaseMessageHandler):

    def __init__(self, url=None):
        self.url = url

    def generate_url(self, **kwargs):
        host = kwargs.get("host")
        port = kwargs.get("port")
        user = kwargs.get("user")
        password = kwargs.get("password")
        self.url = f"amqp://{user}:{password}@{host}:{port}/"

    def create(self):
        parms = pika.URLParameters(self.url)
        connection = pika.BlockingConnection(parms)
        return connection.channel()
