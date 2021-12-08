import pytest
import pandas as pd
from datetime import datetime
from recsys.core import preprocessing


@pytest.fixture
def reviews():
    data = [
        {
            'id': 1,
            'helpfulness': '\n 171 out of 185 found this helpful.\n',
            'date': '31 January 2020'
        },
        {
            'id': 2,
            'helpfulness': '\n 1,710 out of 1,850 found this helpful.\n',
            'date': '31 January 2021'
        }
    ]
    return pd.DataFrame(data)


def test_split_helpfulness_col(reviews):
    df_ = preprocessing.split_helpfulness_col(reviews)

    assert 'helpfulness' not in df_.columns
    assert ('upvotes' in df_.columns) and ('total_votes' in df_.columns)
    assert (df_.loc[0, 'upvotes'] == 171
            and df_.loc[0, 'total_votes'] == 185)
    assert (df_.loc[1, 'upvotes'] == 1710
            and df_.loc[1, 'total_votes'] == 1850)


def test_convert_to_date(reviews):
    df_ = preprocessing.convert_to_date(reviews)

    assert 'review_date' in df_.columns
    assert 'date' not in df_.columns
    assert df_.loc[0, 'review_date'] == datetime(2020, 1, 31).date()


def test_extract_substrings_after_anchors_right_number_of_anchors():
    s = 'anchor1 some string anchor2 another string'
    anchors = ['anchor1', 'anchor2']
    substrings = preprocessing.extract_substrings_after_anchors(
        s, anchors
    )
    assert all(anchor in substrings.keys() for anchor in anchors)


def test_extract_substrings_after_anchors_greater_number_of_anchors():
    s = 'anchor1 some string anchor2 another string'
    anchors = ['anchor1', 'anchor2', 'anchor3']
    substrings = preprocessing.extract_substrings_after_anchors(
        s, anchors
    )
    assert all(anchor in substrings.keys() for anchor in anchors)
    assert substrings['anchor3'] is None


def test_extract_substrings_after_anchors_no_anchors_in_string():
    s = 'some string another string'
    anchors = ['anchor1', 'anchor2', 'anchor3']
    substrings = preprocessing.extract_substrings_after_anchors(
        s, anchors
    )
    assert all(anchor in substrings.keys() for anchor in anchors)
    assert all(substrings[anchor] is None for anchor in anchors)
