"""
The entrypoint for the logger
"""

import os

from config.config import load_config, load_env
from message_broker.connect import Connection2
from utils.constants import Config


def settings():
    load_env(os.environ.get(Config.SECRET_PATH.value, Config.SECRET_PATH_DEFAULT.value))
    return load_config(
        os.environ.get(Config.ENV.value), os.environ.get(Config.CONFIG_PATH.value)
    )


if __name__ == "__main__":
    conn = Connection2(config=settings())
    conn.run()
