import os
import io
import time
import random
import dill
import yaml
import csv
import json
import requests
from dataclasses import dataclass
from io import BytesIO
from PIL import Image
from logging import Logger, basicConfig, getLogger
from typing import Dict, Any, Iterable, List
from tenacity import (retry, wait_random,
                      stop_after_attempt,
                      retry_if_exception_type)
import pandas as pd


DIR_PATH = os.path.dirname(__file__)
ROOT_PATH = os.path.abspath(os.path.join(DIR_PATH, '..'))
RETRY_ATTEMPTS = 5
RETRY_MIN_DELAY = 1
RETRY_MAX_DELAY = 2


@dataclass
class SaveResponse:
    is_successful: bool = True
    exception_msg: str = ''


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


def get_full_path(dirname_or_filename: str, filename: str = None) -> str:
    path_norm = os.path.normpath(dirname_or_filename)
    path_tokens = path_norm.split(os.sep)
    if not filename:
        return os.path.join(ROOT_PATH, *path_tokens)
    return os.path.join(ROOT_PATH, *path_tokens,
                        filename if filename else '')


def dump_obj(obj: Any, path: str, mode: str = 'wb') -> None:
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, mode) as output_file:
        dill.dump(obj, output_file)


def load_obj(path: str, mode: str = 'rb') -> Any:
    with open(path, mode) as input_file:
        return dill.load(input_file)


def write_json(obj: Dict[Any, Any], path: str):
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        raise OSError(f'Directory {dirname} does not exist')

    with open(path, 'w') as fp:
        json.dump(obj, fp)


def read_json(path: str) -> Dict[Any, Any]:
    dirname = os.path.dirname(path)
    if not os.path.exists(dirname):
        raise OSError(f'Directory {dirname} does not exist')

    data = None
    with open(path, 'r') as fp:
        data = json.load(fp)
    return data


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


def create_logger(filename: str, msg_format: str, dt_format: str, level: str)\
        -> Logger:
    logger_params = {
        'filename': get_full_path(filename),
        'format': msg_format,
        'datefmt': dt_format,
        'level': level
    }
    basicConfig(**logger_params)
    return getLogger('')


@retry(
    retry=retry_if_exception_type((requests.ConnectionError,
                                   requests.Timeout)),
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_random(RETRY_MIN_DELAY, RETRY_MAX_DELAY)
)
def send_request(url: str, session: requests.Session = None,
                 **request_params) -> requests.Response:
    headers = {"Accept-Language": "en-US,en;q=0.5"}
    if session:
        return session.get(url, headers=headers, **request_params)
    return requests.get(url, headers=headers, **request_params)


def save_df(df: pd.DataFrame, filename: str, config: Dict[str, Any],
            credentials: Dict[str, str] = None) -> SaveResponse:
    if config['s3_bucket']:
        if not credentials:
            return SaveResponse(False, 'No credentials were passed')
        try:
            filename_full = config["metadata_dir"] + '/' + filename
            path = f's3://{config["s3_bucket"]}/{filename_full}'
            options = {
                'key': credentials['access_key'],
                'secret': credentials['secret_access_key']
            }
            df.to_csv(path, storage_options=options)
            return SaveResponse()
        except Exception as e:
            return SaveResponse(False, str(e))

    try:
        path = os.path.join('data', config['metadata_dir'], filename)
        df.to_csv(path)
        return SaveResponse()
    except Exception as e:
        return SaveResponse(False, str(e))


def save_img(img: bytes, img_name: str, config: Dict[str, Any],
             s3_client=None) -> SaveResponse:
    img_b = Image.open(BytesIO(img))
    if config['s3_bucket']:
        if not s3_client:
            return SaveResponse(False, 'No credentials were passed')
        try:
            img_name_full = os.path.join(config["s3_poster_dir"], img_name)
            buffer = BytesIO()
            img_b.save(buffer, "JPEG")
            buffer.seek(0)

            response = s3_client.put_object(
                Bucket=config['s3_bucket'],
                Key=img_name_full,
                Body=buffer,
                ContentType='image/jpeg',
            )
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return SaveResponse()
            else:
                return SaveResponse(False, 'Bad status code')
        except Exception as e:
            return SaveResponse(False, str(e))

    try:
        img_name_full = os.path.join('data', config['poster_dir'], img_name)
        img_b.save(img_name_full)
        return SaveResponse()
    except Exception as e:
        return SaveResponse(False, str(e))
