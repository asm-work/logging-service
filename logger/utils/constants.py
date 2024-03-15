from enum import Enum

class Config(Enum):
    MQ_URL = 'MQ_URL'
    MQ_HOST = 'MQ_HOST'
    MQ_PORT = 'MQ_PORT'
    MQ_USER = 'MQ_USER'
    MQ_PASS = 'MQ_PASS'
    # Default values
    MQ_HOST_DEFAULT = 'localhost'
    MQ_PORT_DEFAULT = 5672
    MQ_USER_DEFAULT = 'guest'
    MQ_PASS_DEFAULT = 'guest'