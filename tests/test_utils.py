import os
import paths  # noqa
from src.utils import get_filepath, dump_obj, load_obj, parse_config


class TestPickle:
    test_obj = [1, 2, 3, 5]
    test_dir = 'tests/test_data'
    test_filename = 'test.obj'
    test_path = get_filepath(test_dir, test_filename)

    def test_dump_obj(self):
        dump_obj(self.test_obj, self.test_path)
        assert os.path.isfile(self.test_path)

    def test_load_obj(self):
        obj = load_obj(self.test_path)
        assert obj == self.test_obj


def test_parse_config():
    cfg = parse_config()
    assert bool(cfg)
