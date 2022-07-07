import os
import bs4 as bs
from recsys.utils import load_obj
from recsys.imdb_parser.identifiers import IDCollector

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
IDENTIFIERS_PAGE_PATH = os.path.join(FILE_DIR, '..', 'data',
                                     'page_with_identifiers.pkl')


def test_collect_movie_id():
    page = load_obj(IDENTIFIERS_PAGE_PATH)
    ids = IDCollector.collect_movie_id(bs.BeautifulSoup(page, 'lxml'))
    some_items = (
        ('/title/tt7991608/', {'main_genre': 'Action'}),
        ('/title/tt10872600/', {'main_genre': 'Action'}),
        ('/title/tt7097896/', {'main_genre': 'Action'})
    )
    assert len(ids) == 50
    assert all([item in list(ids.items()) for item in some_items])
