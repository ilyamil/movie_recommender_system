import os
import time
import random
import pickle
import yaml
from typing import Dict, Any

DIR_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.abspath(os.path.join(DIR_PATH, '..'))
# CONFIG_PATH = os.path.join(DIR_PATH, 'config.yaml')


def wait(min_time: int, max_time: int = None) -> None:
    if max_time:
        sleep_for = random.randint(min_time, max_time)
    else:
        sleep_for = min_time
    time.sleep(sleep_for)


def get_filepath(dirname: str, filename: str) -> str:
    dirname_tokens = os.path.split(dirname)
    return os.path.join(ROOT_PATH, *dirname_tokens, filename)


def dump_obj(obj, path: str):
    with open(path, 'wb') as output_file:
        try:
            pickle.dump(obj, output_file)
        except Exception as e:
            print(e)


def load_obj(path: str):
    with open(path, 'rb') as input_file:
        try:
            return pickle.load(input_file)
        except Exception as e:
            print(e)


def parse_config(path: str) -> Dict[str, Any]:
    with open(path, 'r') as configs:
        try:
            cfg_parsed = yaml.safe_load(configs)
            return cfg_parsed
        except yaml.YAMLError as e:
            raise e
