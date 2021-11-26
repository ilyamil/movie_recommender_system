import os
import time
import random
import pickle
import yaml
import logging
import csv
import requests
from typing import Dict, Any, Iterable
from tenacity import (retry, wait_random,
                      stop_after_attempt,
                      retry_if_exception_type)


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


def dump_obj(obj, path: str) -> None:
    with open(path, 'wb') as output_file:
        pickle.dump(obj, output_file)


def load_obj(path: str) -> Any:
    with open(path, 'rb') as input_file:
        return pickle.load(input_file)


def write_csv(data: Iterable[Dict[str, Any]], path: str,
              fieldnames: str = 'infer', mode: str = 'a',
              encoding: str = 'utf8') -> None:
    with open(path, mode, newline='', encoding=encoding) as file:
        if fieldnames == 'infer':
            fieldnames_ = list(data[0].keys())
        else:
            fieldnames_ = fieldnames
        writer = csv.DictWriter(file, fieldnames_)
        writer.writeheader()
        writer.writerows(data)


def read_csv(path, encoding: str = 'utf8'):
    with open(path, 'r', newline='', encoding=encoding) as file:
        return list(csv.DictReader(file))


def parse_config(path: str, *sections: str) -> Dict[str, Any]:
    with open(path, 'r') as config:
        parsed_config = yaml.safe_load(config)
        if sections:
            for section in sections:
                parsed_config = parsed_config[section]
        return parsed_config


def create_logger(cfg: Dict[str, Any], write_file: str) -> logging.Logger:
    logging.basicConfig(
        filename=get_full_path(cfg['dir'], write_file),
        format=cfg['msg_format'],
        datefmt=cfg['dt_format'],
        level=cfg['level']
    )
    return logging.getLogger('')


@retry(
    retry=retry_if_exception_type((requests.ConnectionError,
                                   requests.Timeout)),
    stop=stop_after_attempt(10), wait=wait_random(1, 2)
)
def send_request(url: str, session: requests.Session = None,
                 **get_params) -> requests.Response:
    if session:
        return session.get(url, **get_params)
    return requests.get(url, **get_params)
