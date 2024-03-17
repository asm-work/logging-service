import os

from config.config import load_config, load_env
from utils.constants import Config, TestConfig


class BaseTest:

    def load_envs(self):
        load_env(
            os.environ.get(
                TestConfig.TEST_SECRET_PATH.value, Config.SECRET_PATH_DEFAULT.value
            )
        )
        self.env = os.environ.get(TestConfig.TEST_ENV.value)
        self.mq_url = os.environ.get(Config.MQ_URL.value)
        self.config_data = load_config(
            self.env, os.environ.get(Config.CONFIG_PATH.value)
        )
