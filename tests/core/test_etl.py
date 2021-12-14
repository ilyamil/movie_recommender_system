import pytest
import pandas as pd
from datetime import datetime
from recsys.core import etl


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


@pytest.fixture
def movie_details():
    data = [
        {
            'title_id': 1,
            'agg_rating': '8.9/1099M',
            'original_title': 'Original title: The Dark Knight',
            'review_summary': ("{'n_user_reviews': '7.7KUser reviews',"
                               " 'n_critic_reviews': '433Critic reviews',"
                               " 'metascore': '84Metascore'}"),
            'tagline': 'TaglinesGood Cop. Mad Cop.',
            'details': (
                'Release dateAugust 14, 2008 (Russia)'
                'Countries of originUnited StatesUnited Kingdom'
                'Official sitescentrumfilmowOfficial Facebook'
                'LanguagesEnglishMandarin'
                'Also known asBatman Begins 2'
                'Filming locationsChicago, Illinois, USA'
                'Production companiesWarner Bros.Legendary'
                ' EntertainmentSyncopy'
            ),
            'boxoffice': (
                'Budget$185,000,000 (estimated)'
                'Gross US & Canada$534,858,444'
                'Opening weekend US & Canada$158,411,483Jul 20, 2008'
                'Gross worldwide$1,005,973,645'
                'See detailed box office info on IMDbPro'
            ),
            'techspecs': (
                'Runtime2 hours 32 minutesSound mixDolby Digital'
                'SDDSDTSAspect ratio2.39 : 1'
            ),
            'actors': (
                "{'Christian Bale': '/name/nm0000288?ref_=tt_cl_t_1',"
                "'Heath Ledger': '/name/nm0005132?ref_=tt_cl_t_2'}"
            ),
            'imdb_recommendations': (
                "['/title/tt1345836/?ref_=tt_sims_tt_t_1',"
                " '/title/tt1375666/?ref_=tt_sims_tt_t_2']"
            )
        }
    ]
    return pd.DataFrame(data)


def test_split_helpfulness_col(reviews):
    df_ = etl.split_helpfulness_col(reviews)

    assert ('upvotes' in df_.columns) and ('total_votes' in df_.columns)
    assert 'helpfulness' not in df_.columns
    assert (df_.loc[0, 'upvotes'] == 171
            and df_.loc[0, 'total_votes'] == 185)
    assert (df_.loc[1, 'upvotes'] == 1710
            and df_.loc[1, 'total_votes'] == 1850)


def test_convert_to_date(reviews):
    df_ = etl.convert_to_date(reviews)

    assert 'review_date' in df_.columns
    assert 'date' not in df_.columns
    assert df_.loc[0, 'review_date'] == datetime(2020, 1, 31).date()


def test_extract_substrings_after_anchors_right_number_of_anchors():
    s = 'anchor1 some string anchor2 another string'
    anchors = ['anchor1', 'anchor2']
    substrings = etl.extract_substrings_after_anchors(
        s, anchors
    )
    assert all(anchor in substrings.keys() for anchor in anchors)


def test_extract_substrings_after_anchors_greater_number_of_anchors():
    s = 'anchor1 some string anchor2 another string'
    anchors = ['anchor1', 'anchor2', 'anchor3']
    substrings = etl.extract_substrings_after_anchors(
        s, anchors
    )
    assert all(anchor in substrings.keys() for anchor in anchors)
    assert substrings['anchor3'] is None


def test_extract_substrings_after_anchors_no_anchors_in_string():
    s = 'some string another string'
    anchors = ['anchor1', 'anchor2', 'anchor3']
    substrings = etl.extract_substrings_after_anchors(
        s, anchors
    )
    assert all(anchor in substrings.keys() for anchor in anchors)
    assert all(substrings[anchor] is None for anchor in anchors)


def test_split_with_capital_letter():
    s = 'United StatesUnited Kingdom'
    tokens = etl.split_with_capital_letter(s)
    assert len(tokens) == 2
    assert tokens == ['United States', 'United Kingdom']


def test_split_aggregate_rating_col(movie_details):
    df_ = etl.split_aggregate_rating_col(movie_details)

    assert ('rating' in df_.columns)\
           and ('total_votes' in df_.columns)
    assert 'agg_rating' not in df_.columns
    assert abs(df_['rating'][0] - 8.9) < 0.0001
    assert df_['total_votes'][0] == 99_000_000


def test_extract_original_title(movie_details):
    df_ = etl.extract_original_title(movie_details)
    assert df_['original_title'][0] == 'The Dark Knight'


def test_split_review_summary(movie_details):
    df_ = etl.split_review_summary(movie_details)
    required_cols = {
        'user_reviews_num': 7700,
        'critic_reviews_num': 433,
        'metascore': 84
    }
    assert all(col in df_.columns for col in required_cols.keys())
    assert all(int(df_[col][0]) == val for col, val in required_cols.items())
    assert 'review_summary' not in df_.columns


def test_extract_tagline(movie_details):
    df_ = etl.extract_tagline(movie_details)
    assert df_['tagline'][0] == 'Good Cop. Mad Cop.'


def test_extract_movie_details(movie_details):
    df_ = etl.extract_movie_details(movie_details)
    required_cols = [
        'release_date',
        'country_of_origin',
        # 'also_known_as': 'Batman Begins 2',
        # 'filming_locations': 'Chicago, Illinois, USA',
        # 'production_companies': 'Warner Bros.Legendary EntertainmentSyncopy'
    ]
    assert all(col in df_.columns for col in required_cols)
    assert df_['release_date'][0] == datetime(2008, 8, 14)
    assert df_['country_of_origin'][0] == ['United States', 'United Kingdom']
    assert 'details' not in df_.columns


def test_extract_boxoffice(movie_details):
    df_ = etl.extract_boxoffice(movie_details)
    assert ('budget' in df_.columns)\
        and ('boxoffice' in df_.columns)
    assert df_['budget'][0] == '$185,000,000'
    assert df_['boxoffice'][0] == '$1,005,973,645'


def test_extract_runtime(movie_details):
    df_ = etl.extract_runtime(movie_details)
    assert 'runtime_min' in df_.columns
    assert df_['runtime_min'][0] == 152
    assert 'techspecs' not in df_.columns


def test_normalize_actors(movie_details):
    actors = etl.normalize_actors(movie_details)
    required_cols = [
        'title_id',
        'actor_id',
        'actor_name',
        'order_num'
    ]
    assert all(col in actors.columns for col in required_cols)
    assert len(actors) == 2
    assert list(actors['title_id'].values) == [1, 1]
    assert list(actors['actor_id'].values) == ['/name/nm0000288/',
                                               '/name/nm0005132/']
    assert list(actors['actor_name'].values) == ['Christian Bale',
                                                 'Heath Ledger']
    assert list(actors['order_num'].values) == [1, 2]


def test_parse_recommendations():
    recomms_raw = {
        'title_id': 1,
        'imdb_recommendations': [
            '/title/tt1345836/?ref_=tt_sims_tt_t_1',
            '/title/tt1375666/?ref_=tt_sims_tt_t_2'
        ]
    }
    recomms_parsed = etl.parse_recommendations(recomms_raw)
    required_cols = ['title_id', 'suggested_title_id', 'order_num']
    assert all(col in recomms_parsed.keys() for col in required_cols)
    assert recomms_parsed['title_id'] == [1, 1]
    assert recomms_parsed['suggested_title_id'] == ['/title/tt1345836/',
                                                    '/title/tt1375666/']
    assert recomms_parsed['order_num'] == [1, 2]


def test_normalize_recommendations(movie_details):
    recomms = etl.normalize_recommendations(movie_details)
    required_cols = ['title_id', 'suggested_title_id', 'order_num']
    assert all(col in recomms.columns for col in required_cols)
    assert len(recomms) == 2
    assert list(recomms['title_id'].values) == [1, 1]
    assert list(recomms['suggested_title_id'].values) == ['/title/tt1345836/',
                                                          '/title/tt1375666/']
    assert list(recomms['order_num'].values) == [1, 2]
