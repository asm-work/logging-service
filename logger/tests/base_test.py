import os
from config.config import load_config, load_env


class BaseTest:

    def load_envs(self):
        load_env(os.environ.get("TEST_SECRET_PATH", "./config/secrets.env"))
        self.env = os.environ.get("TEST_ENV")
        self.mq_url = os.environ.get('MQ_URL')
        self.config_data = load_config(self.env, os.environ.get("CONFIG_PATH"))
