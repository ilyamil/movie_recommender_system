import os
import logging

import requests
from recsys import utils


class TestPickle:
    test_obj = [1, 2, 3, 5]
    test_dir = 'tests/test_data'
    test_filename = 'test.obj'
    test_path = utils.get_full_path(test_dir, test_filename)

    def test_dump_obj(self):
        utils.dump_obj(self.test_obj, self.test_path)
        assert os.path.isfile(self.test_path)

    def test_load_obj(self):
        obj = utils.load_obj(self.test_path)
        assert obj == self.test_obj


class TestConfigParser:
    dirname = 'scripts'
    filename = 'config.yaml'
    path = utils.get_full_path(dirname, filename)

    def test_parse_config(self):
        cfg = utils.parse_config(self.path)
        assert bool(cfg)

    def test_parse_config_with_sections(self):
        sections = ['data_collection', 'logger']
        cfg = utils.parse_config(self.path, *sections)
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
    path = os.path.join('test_data', 'test_csv.csv')
    utils.write_csv(data, path, fieldnames=fieldnames)

    assert os.path.isfile(path)
    assert data == utils.read_csv(path)
    os.remove(path)


def test_get_response():
    url = 'https://google.com'
    assert utils.get_response(url).status_code == 200
