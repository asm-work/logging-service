class ConnectionCallback:
    def __init__(self) -> None:
        pass

    def on_connection_open(self):
        pass

    def on_connection_open_error(self):
        pass

    def on_connection_closed(self):
        pass


class ChannelCallback:
    def __init__(self) -> None:
        pass

    def on_channel_open(self):
        pass

    def on_channel_closed(self):
        pass


class ExchangeCallback:
    def __init__(self) -> None:
        pass

    def on_exchange_declare(self):
        pass


class QueueCallback:
    def __init__(self) -> None:
        pass

    def on_queue_declare(self):
        pass

    def on_queue_bind(self):
        pass

    def on_qos_ok(self):
        pass


class ConsumerCallback:
    def __init__(self) -> None:
        pass

    def on_consumer_cancelled(self):
        pass

    def on_cancel_ok(self):
        pass


class MessageCallback:
    def __init__(self) -> None:
        pass

    def on_message(self):
        pass
