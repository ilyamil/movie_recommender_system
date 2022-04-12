import os
import sys
import pytest
from bs4 import BeautifulSoup
from recsys.utils import send_request, dump_obj, load_obj
from recsys.imdb_parser import details


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
POSTER_PATH = os.path.join(FILE_DIR, '..', 'data',
                           'poster.pkl')
EXAMPLE_URL = 'https://www.imdb.com/title/tt7991608/'


@pytest.fixture
def page() -> BeautifulSoup:
    page = send_request(EXAMPLE_URL)
    return BeautifulSoup(page.text, 'lxml')


def test_collect_original_title(page):
    orig_title = details.collect_original_title(page)
    assert orig_title is not None
    assert orig_title == 'Original title: Red Notice'


def test_collect_poster(page):
    poster = details.collect_poster(page)
    assert poster is not None
    assert sys.getsizeof(poster) > 1024


def test_collect_review_summary(page):
    rev_content = details.collect_review_summary(page)
    summary_fields = ['user_reviews_num', 'critic_review_num', 'metascore']
    assert all(key in rev_content for key in summary_fields)
    assert all(len(v) > 0 for v in rev_content.values())


def test_collect_aggregate_rating(page):
    rating = details.collect_aggregate_rating(page)
    assert rating is not None
    assert len(rating) > 0
    assert '/' in rating


def test_collect_actors(page):
    actors = details.collect_actors(page)
    true_actors = ['Dwayne Johnson', 'Gal Gadot']
    assert actors is not None
    assert all(actor in actors.keys() for actor in true_actors)


def test_collect_imdb_recommendations(page):
    recommendations = details.collect_imdb_recommendations(page)
    assert recommendations is not None
    assert len(recommendations) > 0


def collect_storyline(page):
    storyline = details.collect_storyline(page)
    assert storyline is not None
    assert len(storyline) > 0


def collect_tagline(page):
    tagline = details.collect_tagline(page)
    assert tagline is not None
    assert tagline == 'Pro and Cons'


def collect_genres(page):
    genres = details.collect_genres(page)
    true_genres = ['Action', 'Adventure', 'Comedy']
    assert genres is not None
    assert all(genre in genres for genre in true_genres)


def test_collect_details(page):
    details_summary = details.collect_details_summary(page)
    sections = [
        'Release date',
        'Country of origin',
        'Official site',
        'Languages',
        'Also known as',
        'Filming locations',
        'Production companies'
    ]
    assert all(section in details_summary for section in sections)


def test_collect_boxoffice(page):
    boxoffice = details.collect_boxoffice(page)
    true_entities = ['Budget', 'Gross worldwide']
    assert boxoffice is not None
    assert all(entity in boxoffice for entity in true_entities)


def test_collect_techspecks(page):
    techspecs = details.collect_techspecs(page)
    true_entities = ['Runtime', 'Color', 'Sound']
    assert techspecs is not None
    assert all(entity in techspecs for entity in true_entities)
