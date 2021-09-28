import os
import time
import random
import pickle
import yaml
import logging
from typing import Dict, Any
from logging import Logger

DIR_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.abspath(os.path.join(DIR_PATH, '..'))


def wait(min_time: int, max_time: int = None) -> None:
    if max_time:
        if min_time > max_time:
            raise ValueError(
                'min_time have to be no greater than max_time'
            )
        sleep_for = random.uniform(min_time, max_time)
    else:
        sleep_for = min_time
    time.sleep(sleep_for)


def get_full_path(dirname: str, filename: str) -> str:
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


def create_logger(logger_cfg: Dict[str, str], filename: str) -> Logger:
    log_level = logger_cfg['level']
    log_dir = logger_cfg['log_dir']
    log_format = logger_cfg['msg_format']
    log_datefmt = logger_cfg['dt_format']
    log_filename = get_full_path(log_dir, filename)
    logging.basicConfig(
        filename=log_filename,
        format=log_format,
        datefmt=log_datefmt,
        level=log_level
    )
    return logging.getLogger('')
