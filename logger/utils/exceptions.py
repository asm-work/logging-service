"""Custom exception classes
"""

from dotmap import DotMap


class EmptyCallbackErr(Exception):

    def __init__(self, callback: object):
        self.message = (
            f"Callback is not preset, expect a callback object, got {callback} instead"
        )
        super().__init__(self.message)


class InvalidConfigErr(Exception):

    def __init__(self, config_key: str, config_val: DotMap):
        self.message = f"Config value is invalid: {config_val}"
        super().__init__(self.message)


class NotConnectedErr(Exception):

    def __init__(self, conn: object) -> None:
        self.message = f"Expect a connection object, got {conn} instead. Make sure to call the connect"
        super().__init__(self.message)


class EmptyChannelErr(Exception):

    def __init__(self, chan: object) -> None:
        self.message = f"Expect a channel object, got {chan} instead. Make sure to call the channel_open"
        super().__init__(self.message)


class EmptyExchangeErr(Exception):

    def __init__(self, exch: object) -> None:
        self.message = f"Expect an exchange object, got {exch} instead. Make sure to declare the exchange"
        super().__init__(self.message)
