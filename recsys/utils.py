import os
import io
import time
import random
import dill
import yaml
import logging
import csv
import requests
from typing import Dict, Any, Iterable, List
from tenacity import (retry, wait_random,
                      stop_after_attempt,
                      retry_if_exception_type)
from PIL import Image


DIR_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.abspath(os.path.join(DIR_PATH, '..'))
RETRY_ATTEMPTS = 5
RETRY_MIN_DELAY = 1
RETRY_MAX_DELAY = 2


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


def get_full_path(dirname: str, filename: str = None) -> str:
    dirname_tokens = os.path.split(dirname)
    if filename:
        return os.path.join(ROOT_PATH, *dirname_tokens, filename)
    return os.path.join(ROOT_PATH, *dirname_tokens)


def dump_obj(obj: Any, path: str, mode: str = 'wb') -> None:
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode) as output_file:
        dill.dump(obj, output_file)


def load_obj(path: str, mode: str = 'rb') -> Any:
    with open(path, mode) as input_file:
        return dill.load(input_file)


def write_csv(data: Iterable[Dict[str, Any]], path: str,
              fieldnames: str = 'infer', mode: str = 'a',
              encoding: str = 'utf8') -> None:
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode, newline='', encoding=encoding) as file:
        if fieldnames == 'infer':
            fieldnames_ = list(data[0].keys())
        else:
            fieldnames_ = fieldnames
        writer = csv.DictWriter(file, fieldnames_)
        writer.writeheader()
        writer.writerows(data)


def read_csv(path, encoding: str = 'utf8') -> List[Dict[str, Any]]:
    with open(path, 'r', newline='', encoding=encoding) as file:
        return list(csv.DictReader(file))


def write_bytest_to_image(img_bytes: bytes, path: str,
                          fmt: str = 'jpeg') -> None:
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    img_stream = io.BytesIO(img_bytes)
    img = Image.open(img_stream)
    img.save(path, fmt)


def parse_config(path: str, *sections: str) -> Dict[str, Any]:
    with open(path, 'r') as config:
        parsed_config = yaml.safe_load(config)
        if sections:
            for section in sections:
                parsed_config = parsed_config[section]
        return parsed_config


def create_logger(cfg: Dict[str, Any], write_file: str) -> logging.Logger:
    logger_params = {
        'filename': get_full_path(cfg['dir'], write_file),
        'format': cfg['msg_format'],
        'datefmt': cfg['dt_format'],
        'level': cfg['level']
    }
    logging.basicConfig(**logger_params)
    return logging.getLogger('')


@retry(
    retry=retry_if_exception_type((requests.ConnectionError,
                                   requests.Timeout)),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_random(RETRY_MIN_DELAY, RETRY_MAX_DELAY)
)
def send_request(url: str, session: requests.Session = None,
                 **request_params) -> requests.Response:
    if session:
        return session.get(url, **request_params)
    return requests.get(url, **request_params)
