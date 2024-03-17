import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from dotmap import DotMap


def _get_env_config(config, env):
    configs = yaml.safe_load(config)
    return configs[env]


def load_config(env: str, config_path):
    # All config files are readed as yaml
    config_files = [
        os.path.join(r, file)
        for r, d, f in os.walk(config_path)
        for file in f
        if ".yaml" in file
    ]
    config = DotMap(
        {Path(cfg).stem: _get_env_config(open(cfg, "r"), env) for cfg in config_files}
    )
    return config


def load_env(path):
    load_dotenv(path)
