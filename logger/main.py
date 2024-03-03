"""
The entrypoint for the application
"""
import pika

if __name__ == "__main__":
    print("Starting  logger .....")
    # TODO: Load the configurations
    # TODO: Use factory method here
    params = pika.URLParameters("amqp://guest:guest@message-broker:5672/")
    # connection = pika.BlockingConnection(pika.ConnectionParameters(host='message-broker'))
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    print("connected to message broker.....")
    print("waiting for receive signal")