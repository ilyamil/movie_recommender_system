import os
import pytest
from bs4 import BeautifulSoup
from recsys.utils import load_obj
from recsys.imdb_parser.reviews import ReviewCollector

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
REVIEW_TAG_PATH = os.path.join(
    FILE_DIR, '..', 'data', 'review_tag_example.pkl'
)
REVIEW_ID = '/title/tt0068646/'


@pytest.fixture
def review():
    review_soup = BeautifulSoup(load_obj(REVIEW_TAG_PATH), 'lxml')
    return ReviewCollector.collect_review(REVIEW_ID, review_soup)


def test_rc_review_parsing(review):
    assert bool(review)


def test_rc_collect_all_review_attributes(review):
    review_attributes = list(review.keys())
    true_attributes = [
        'id',
        'text',
        'rating',
        'date',
        'title',
        'author',
        'helpfulness'
    ]
    assert review_attributes == true_attributes


def test_rc_collect_id(review):
    assert review['id'] == REVIEW_ID


def test_rc_collect_date(review):
    true_date = '\n    30 March 2013\n   '
    assert review['date'] == true_date


def test_rc_collect_title(review):
    true_title = (
        '\n   #2 on IMDb\'s Top 100, a multi-Oscar-winner'
        'and over 1500 reviews...what more can I say about the movie?!\n  '
    )
    true_title_strip = (
        true_title
        .replace('\n', '')
        .replace(' ', '')
    )
    review_title_strip = (
        review['title']
        .replace('\n', '')
        .replace(' ', '')
    )
    assert review_title_strip == true_title_strip
