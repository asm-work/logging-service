class Channel:
    def __init__(self):
        pass

    def close(self):
        pass

    def add_on_close_callback(self, callback):
        pass

    def add_on_cancel_callback(self, callback):
        pass

    def exchange_declare(self, exchange, exchange_type, callback):
        pass

    def queue_declare(self, queue, callback):
        pass

    def queue_bind(self, queue, exchange, routing_key, callback):
        pass

    def basic_qos(self, prefetch_count, callback):
        pass

    def basic_consume(self, queue, on_message_callback, auto_ack):
        pass

    def basic_cancel(self, consumer_tag, callback):
        pass


class Connection:
    def __init__(self):
        pass

    def channel(self):
        return Channel()


class IoLoop:
    def __init__(self) -> None:
        pass

    def start(self):
        pass

    def stop(self):
        pass


class SelectConnection:
    def __init__(self) -> None:
        self.ioloop = IoLoop()

    def close(self):
        pass

    def channel(self, on_open_callback):
        return Channel()
