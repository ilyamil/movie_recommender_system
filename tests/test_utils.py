import os
import logging

from bs4 import BeautifulSoup
from recsys import utils
from recsys.imdb_parser.details import collect_poster


CONFIG_DIR = 'scripts'
CONFIG_FILE = 'config.yaml'
EXAMPLE_URL = 'https://www.imdb.com/title/tt0068646/'


def test_obj_io():
    obj = [1, 2, 3, 5]
    path = utils.get_full_path(os.path.join('tests', 'data'),
                               'tmp.pkl')
    utils.dump_obj(obj, path)
    assert os.path.isfile(path)

    obj_loaded = utils.load_obj(path)
    assert obj_loaded == obj

    os.remove(path)


def test_parse_config():
    cfg_path = utils.get_full_path(CONFIG_DIR, CONFIG_FILE)
    cfg = utils.parse_config(cfg_path)
    assert bool(cfg)


def test_parse_config_with_sections():
    sections = ['data_collection', 'reviews']
    cfg_path = utils.get_full_path(CONFIG_DIR, CONFIG_FILE)
    cfg = utils.parse_config(cfg_path, *sections)
    assert bool(cfg)


def test_logger_init():
    cfg = {
        'dir': 'some_dir',
        'msg_format': '%(asctime)s %(levelname)s %(message)s',
        'dt_format': '%Y-%m-%d %H:%M:%S',
        'level': 'INFO'
    }
    logger = utils.create_logger(cfg, 'some_file.log')
    assert isinstance(logger, logging.Logger)


def test_csv_io():
    fieldnames = ['first_name', 'last_name']
    data = [
            {'first_name': 'John', 'last_name': 'Doe'},
            {'first_name': 'Harry', 'last_name': 'Potter'}
    ]
    path = os.path.join('data', 'csv.csv')
    utils.write_csv(data, path, fieldnames=fieldnames)
    assert os.path.isfile(path)
    assert data == utils.read_csv(path)
    os.remove(path)


def test_write_bytest_to_image():
    page = utils.send_request(EXAMPLE_URL)
    poster = collect_poster(BeautifulSoup(page.content, 'lxml'))
    path = utils.get_full_path(os.path.join('tests', 'data'),
                               'tmp_img.jpeg')
    utils.write_bytest_to_image(poster, path)
    assert os.path.isfile(path)
    os.remove(path)


def test_send_request():
    url = 'https://google.com'
    assert utils.send_request(url).status_code == 200
