import os
from recsys.utils import load_obj
from recsys.imdb_parser.identifiers import IDCollector

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
IDENTIFIERS_PAGE_PATH = os.path.join(FILE_DIR, '..', 'data',
                                     'page_with_identifiers.pkl')


def test_collect_movie_id():
    page = load_obj(IDENTIFIERS_PAGE_PATH)
    ids = IDCollector.collect_movie_id(page)
    assert len(ids) == 50
