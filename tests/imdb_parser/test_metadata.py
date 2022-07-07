import os
import sys
import pytest
from bs4 import BeautifulSoup
from recsys.utils import send_request
from recsys.imdb_parser import metadata


FILE_DIR = os.path.dirname(os.path.realpath(__file__))
POSTER_PATH = os.path.join(FILE_DIR, '..', 'data',
                           'poster.pkl')
EXAMPLE_URL = 'https://www.imdb.com/title/tt7991608/'


@pytest.fixture
def page() -> BeautifulSoup:
    page = send_request(EXAMPLE_URL)
    return BeautifulSoup(page.text, 'lxml')


def test_collect_original_title(page):
    orig_title = metadata.collect_original_title(page)
    assert orig_title is not None
    assert orig_title == 'Red Notice'


def test_collect_poster_url(page):
    poster_url = metadata.collect_poster_url(page)
    assert poster_url is not None
    assert 'https://m.media-amazon.com/images/M/' in poster_url
    assert '.jpg' in poster_url


def test_collect_review_summary(page):
    rev_content = metadata.collect_review_summary(page)
    summary_fields = ['user_review_num', 'critic_review_num', 'metascore']
    assert all(key in rev_content for key in summary_fields)
    assert all(len(v) > 0 for v in rev_content.values())


def test_collect_aggregate_rating(page):
    rating = metadata.collect_aggregate_rating(page)
    {'avg_rating': '9.0/10', 'num_votes': '2.6M'}
    assert rating is not None
    assert len(rating) == 2
    assert '/10' in rating['avg_rating']


def test_collect_actors(page):
    actors = metadata.collect_actors(page)
    true_actors = ['Dwayne Johnson', 'Gal Gadot']
    assert actors is not None
    assert all(actor in actors.keys() for actor in true_actors)


def test_collect_imdb_recommendations(page):
    recommendations = metadata.collect_imdb_recommendations(page)
    assert recommendations is not None
    assert len(recommendations) > 0


def test_collect_genres(page):
    genres = metadata.collect_genres(page)
    true_genres = {'Action', 'Adventure', 'Comedy'}
    assert genres is not None
    assert true_genres.intersection(genres)


def test_collect_details(page):
    details_summary = metadata.collect_details_summary(page)
    sections = [
        'release_date',
        'countries_of_origin',
        'language',
        'also_known_as',
        'production_companies',
        'filming_locations',
        'runtime'
    ]
    assert all(section in details_summary.keys() for section in sections)


def test_collect_boxoffice(page):
    boxoffice = metadata.collect_boxoffice(page)
    assert boxoffice is not None
    assert 'budget' in boxoffice.keys()
    assert boxoffice['budget'] == '$160,000,000 (estimated)'
