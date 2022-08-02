import pytest
from datetime import datetime
import pandas as pd
from recsys.imdb_parser import etl


@pytest.fixture
def movie_details():
    data = [{
        'title_id': 1,
        'agg_rating': {
            'avg_rating': '7.0/10',
            'num_votes': '2.9K'
        },
        'genres': ['Adventure', 'Drama', 'Fantasy'],
        'original_title': 'The Dark Knight',
        'review_summary': {
            'user_review_num': '1.2K',
            'critic_review_num': '76',
            'metascore': '99'
        },
        'tagline': 'TaglinesGood Cop. Mad Cop.',
        'details': {
            'release_date': ['July 1911 (United States)'],
            'countries_of_origin': ['Italy'],
            'language': ['Italian', 'English'],
            'also_known_as': ['El infierno'],
            'production_companies': ['Milano Film', 'SAFFI-Comerio'],
            'filming_locations': ['Bovisa, Milano, Lombardia, Italy'],
            'runtime': '1 hour 11 minutes'
        },
        'boxoffice': {
            'budget': '$185,000,000 (estimated)',
            'boxoffice_gross_domestic': '$534,987,076',
            'boxoffice_gross_opening': '$158,411,483',
            'boxoffice_gross_worldwide': '$1,006,102,277'
        },
        'actors': {
            '1': '/name/nm0660139',
            '2': '/name/nm0685283',
            '3': '/name/nm0685284'
        },
        'imdb_recommendations': {
            '1': '/title/tt0003740/',
            '2': '/title/tt0001223/',
        }
    }]
    return pd.DataFrame(data)


def test_normalize(movie_details):
    recomms = etl.normalize(movie_details, 'imdb_recommendations')
    actors = etl.normalize(movie_details, 'actors')

    assert all(c in recomms.columns for c in [1, 2])
    assert (recomms.loc[0, 1] == '/title/tt0003740/')\
           & (recomms.loc[0, 2] == '/title/tt0001223/')

    assert all(c in actors.columns for c in [1, 2, 3])
    assert (actors.loc[0, 1] == '/name/nm0660139')\
           & (actors.loc[0, 2] == '/name/nm0685283')\
           & (actors.loc[0, 3] == '/name/nm0685284')


def test_review_summary(movie_details):
    summary = etl.split_review_summary(movie_details)

    check_cols = ['user_review_num', 'critic_review_num', 'metascore']
    assert all(c in summary.columns for c in check_cols)
    assert (summary.at[0, 'user_review_num'] == 1200.)\
           & (summary.at[0, 'critic_review_num'] == 76.)\
           & (summary.at[0, 'metascore'] == 99.)


def test_avg_rating(movie_details):
    rating = etl.split_aggregate_rating(movie_details)

    assert 'rating' in rating.columns and 'num_votes' in rating.columns
    assert (rating.at[0, 'rating'] == 7.0)\
           & (rating.at[0, 'num_votes'] == 2900.)


def test_split_aggregate_rating(movie_details):
    df_ = etl.split_aggregate_rating(movie_details)

    assert ('rating' in df_.columns)\
           and ('num_votes' in df_.columns)
    assert 'agg_rating' not in df_.columns
    assert abs(df_.at[0, 'rating'] - 7.0) < 1e-6
    assert abs(df_.at[0, 'num_votes'] - 2900) < 1e6


def test_split_review_summary(movie_details):
    df_ = etl.split_review_summary(movie_details)
    required_cols = ['user_review_num', 'critic_review_num', 'metascore']
    assert all(col in df_.columns for col in required_cols)
    assert abs(df_.at[0, 'user_review_num'] - 1200) < 1e-6
    assert df_.at[0, 'critic_review_num'] == 76
    assert df_.at[0, 'metascore'] == 99
    assert 'review_summary' not in df_.columns


def test_format_release_date(movie_details):
    df_norm = pd.json_normalize(movie_details['details'])
    fmt_release_date = etl.format_release_date(df_norm)
    assert fmt_release_date.at[0, 'release_date'].to_pydatetime()\
           == datetime(1911, 7, 1, 0, 0)


def test_split_countries_of_origin(movie_details):
    df_norm = pd.json_normalize(movie_details['details'])
    df_ = etl.split_countries_of_origin(df_norm)
    assert (df_.at[0, 'country_of_origin_1'] == 'Italy')\
           & (df_.at[0, 'country_of_origin_2'] is None)\
           & (df_.at[0, 'country_of_origin_3'] is None)
    assert 'countries_of_origin' not in df_.columns


def test_split_languages(movie_details):
    df_norm = pd.json_normalize(movie_details['details'])
    df_ = etl.split_language(df_norm)
    assert df_.at[0, 'original_language'] == 'Italian'
    assert 'language' not in df_.columns


def test_format_aka(movie_details):
    df_norm = pd.json_normalize(movie_details['details'])
    df_ = etl.format_aka(df_norm)
    assert df_.at[0, 'also_known_as'] == 'El infierno'


def test_split_production_companies(movie_details):
    df_norm = pd.json_normalize(movie_details['details'])
    df_ = etl.split_production_companies(df_norm)
    assert df_.at[0, 'production_company_1'] == 'Milano Film'
    assert df_.at[0, 'production_company_2'] == 'SAFFI-Comerio'
    assert 'production_companies' not in df_.columns


def test_split_filming_locations(movie_details):
    df_norm = pd.json_normalize(movie_details['details'])
    df_ = etl.split_filming_locations(df_norm)
    assert df_.at[0, 'filming_location'] == 'Bovisa, Milano, Lombardia, Italy'
    assert df_.at[0, 'filming_country'] == 'Italy'
    assert 'filming_locations' not in df_.columns


def test_split_genres(movie_details):
    df_genres = etl.split_movie_genres(movie_details)
    assert df_genres.at[0, 'genre_1'] == 'Adventure'
    assert df_genres.at[0, 'genre_2'] == 'Drama'
    assert df_genres.at[0, 'genre_3'] == 'Fantasy'
    assert 'genres' not in df_genres.columns


def test_split_boxoffice(movie_details):
    df_ = etl.split_boxoffice(movie_details)
    required_cols = [
        'budget',
        'boxoffice_gross_domestic',
        'boxoffice_gross_opening',
        'boxoffice_gross_worldwide'
    ]
    assert all(c in df_.columns for c in required_cols)
    assert df_.at[0, 'budget'] == '$185,000,000 (estimated)'
    assert df_.at[0, 'boxoffice_gross_domestic'] == '$534,987,076'
    assert df_.at[0, 'boxoffice_gross_opening'] == '$158,411,483'
    assert df_.at[0, 'boxoffice_gross_worldwide'] == '$1,006,102,277'
