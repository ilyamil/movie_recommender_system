import os
import sys
import pytest
from bs4 import BeautifulSoup
from recsys.utils import send_request, dump_obj, load_obj
from recsys.imdb_parser.details import DetailsCollector


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
POSTER_PATH = os.path.join(FILE_DIR, '..', 'data',
                           'poster.pkl')
EXAMPLE_URL = 'https://www.imdb.com/title/tt7991608/'


@pytest.fixture
def example_page() -> BeautifulSoup:
    page = send_request(EXAMPLE_URL)
    return BeautifulSoup(page.text, 'lxml')


def test_collect_original_title(example_page):
    orig_title = DetailsCollector.collect_original_title(example_page)
    assert orig_title is not None
    assert orig_title == 'Original title: Red Notice'


def test_collect_poster(example_page):
    poster = DetailsCollector.collect_poster(example_page)
    assert poster is not None
    assert sys.getsizeof(poster) > 1024


def test_collect_review_content(example_page):
    rev_content = DetailsCollector.collect_review_content(example_page)
    content_fields = [
        'n_user_reviews',
        'n_critic_reviews',
        'metascore'
    ]
    assert all(key in rev_content for key in content_fields)
    assert all(len(v) > 0 for v in rev_content.values())


def test_collect_aggregate_rating(example_page):
    rating = DetailsCollector.collect_aggregate_rating(example_page)
    assert rating is not None
    assert len(rating) > 0
    assert '/' in rating
