import pathlib
import tomllib
from logging import config

config_file = pathlib.Path("logging.toml")

if config_file.exists():
    config.dictConfig(tomllib.loads(config_file.read_text()))
