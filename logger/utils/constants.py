from enum import Enum, auto


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
    # Logging
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s - %(name)s"
    LOG_FILE_HANDLER = "file"
    LOG_STREAM_HANDLER = "stream"
    LOG_FILE_DIR = "./logs"
    LOG_FILE_NAME = "event-logs.log"


class TestConfig(Enum):
    TEST_SECRET_PATH = "TEST_SECRET_PATH"
    TEST_ENV = "TEST_ENV"


class GenericConstants(Enum):
    # Logger
    CONSOLE_LOGGER = auto()
    GENERIC_LOGGER = auto()
    # MessageBroker
    RABBIT_MQ_CLIENT = auto()
    URL_CONN_METHOD = auto()
    PARAM_CONN_METHOD = auto()
