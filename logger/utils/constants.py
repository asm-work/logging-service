from enum import Enum


class Config(Enum):
    # Config
    CONFIG_PATH = "CONFIG_PATH"
    SECRET_PATH = "SECRET_PATH"
    ENV = "ENV"
    # Secret default
    SECRET_PATH_DEFAULT = "./config/secrets.env"
    # MQ
    MQ_URL = "MQ_URL"
    MQ_HOST = "MQ_HOST"
    MQ_PORT = "MQ_PORT"
    MQ_USER = "MQ_USER"
    MQ_PASS = "MQ_PASS"
    # MQ Default values
    MQ_HOST_DEFAULT = "localhost"
    MQ_PORT_DEFAULT = 5672
    MQ_USER_DEFAULT = "guest"
    MQ_PASS_DEFAULT = "guest"


class TestConfig(Enum):
    TEST_SECRET_PATH = "TEST_SECRET_PATH"
    TEST_ENV = "TEST_ENV"
