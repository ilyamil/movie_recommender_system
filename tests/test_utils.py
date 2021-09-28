import os
from recsys.utils import get_full_path, dump_obj, load_obj, parse_config


class TestPickle:
    test_obj = [1, 2, 3, 5]
    test_dir = 'tests/test_data'
    test_filename = 'test.obj'
    test_path = get_full_path(test_dir, test_filename)

    def test_dump_obj(self):
        dump_obj(self.test_obj, self.test_path)
        assert os.path.isfile(self.test_path)

    def test_load_obj(self):
        obj = load_obj(self.test_path)
        assert obj == self.test_obj


def test_parse_config():
    cfg_path = get_full_path('scripts', 'config.yaml')
    cfg = parse_config(cfg_path)
    assert bool(cfg)
