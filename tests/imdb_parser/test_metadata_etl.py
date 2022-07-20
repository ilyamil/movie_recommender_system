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
        'original_title': 'The Dark Knight',
        'review_summary': {
            'user_review_num': '1.2K',
            'critic_review_num': '76',
            'metascore': '99'
        },
        'tagline': 'TaglinesGood Cop. Mad Cop.',
        'details': (
            'Release dateAugust 14, 2008 (Russia)'
            'Countries of originUnited StatesUnited Kingdom'
            'Official sitescentrumfilmowOfficial Facebook'
            'LanguagesEnglishMandarin'
            'Also known asBatman Begins 2'
            'Filming locationsChicago, Illinois, USA'
            'Production companiesWarner Bros.Legendary'
            ' EntertainmentSyncopySee more'
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


# def test_split_aggregate_rating_col(movie_details):
#     df_ = etl.split_aggregate_rating_col(movie_details)

#     assert ('rating' in df_.columns)\
#            and ('total_votes' in df_.columns)
#     assert 'agg_rating' not in df_.columns
#     assert abs(df_['rating'][0] - 8.9) < 0.0001
#     assert df_['total_votes'][0] == 99_000_000


# def test_split_review_summary(movie_details):
#     df_ = etl.split_review_summary(movie_details)
#     required_cols = {
#         'user_review_num': 125,
#         'critic_review_num': 76,
#         'metascore': 99
#     }
#     assert all(col in df_.columns for col in required_cols.keys())
#     assert all(int(df_[col][0]) == val for col, val in required_cols.items())
#     assert 'review_summary' not in df_.columns


# def test_extract_movie_details(movie_details):
#     df_ = etl.extract_movie_details(movie_details)
#     required_cols = [
#         'release_date',
#         'country_of_origin',
#         'production_company'
#         # 'also_known_as': 'Batman Begins 2',
#         # 'filming_locations': 'Chicago, Illinois, USA',
#         # 'production_companies': 'Warner Bros.Legendary EntertainmentSyncopy'
#     ]
#     assert all(col in df_.columns for col in required_cols)
#     assert df_['release_date'][0] == datetime(2008, 8, 14)
#     assert df_['country_of_origin'][0] == ['United States', 'United Kingdom']
#     assert df_['production_company'][0] == ['Warner Bros.',
#                                             'Legendary Entertainment',
#                                             'Syncopy']
#     assert 'details' not in df_.columns


# def test_extract_boxoffice(movie_details):
#     df_ = etl.extract_boxoffice(movie_details)
#     assert ('budget' in df_.columns)\
#         and ('boxoffice' in df_.columns)
#     assert df_['budget'][0] == '$185,000,000'
#     assert df_['boxoffice'][0] == '$1,005,973,645'


# def test_extract_runtime(movie_details):
#     df_ = etl.extract_runtime(movie_details)
#     assert 'runtime_min' in df_.columns
#     assert df_['runtime_min'][0] == 152
#     assert 'techspecs' not in df_.columns


# def test_normalize_actors(movie_details):
#     actors = etl.normalize_actors(movie_details)
#     required_cols = [
#         'title_id',
#         'actor_id',
#         'actor_name',
#         'order_num'
#     ]
#     assert all(col in actors.columns for col in required_cols)
#     assert len(actors) == 2
#     assert list(actors['title_id'].values) == [1, 1]
#     assert list(actors['actor_id'].values) == ['/name/nm0000288/',
#                                                '/name/nm0005132/']
#     assert list(actors['actor_name'].values) == ['Christian Bale',
#                                                  'Heath Ledger']
#     assert list(actors['order_num'].values) == [1, 2]


# def test_parse_recommendations():
#     recomms_raw = {
#         'title_id': 1,
#         'imdb_recommendations': [
#             '/title/tt1345836/?ref_=tt_sims_tt_t_1',
#             '/title/tt1375666/?ref_=tt_sims_tt_t_2'
#         ]
#     }
#     recomms_parsed = etl.parse_recommendations(recomms_raw)
#     required_cols = ['title_id', 'suggested_title_id', 'order_num']
#     assert all(col in recomms_parsed.keys() for col in required_cols)
#     assert recomms_parsed['title_id'] == [1, 1]
#     assert recomms_parsed['suggested_title_id'] == ['/title/tt1345836/',
#                                                     '/title/tt1375666/']
#     assert recomms_parsed['order_num'] == [1, 2]


# def test_normalize_recommendations(movie_details):
#     recomms = etl.normalize_recommendations(movie_details)
#     required_cols = ['title_id', 'suggested_title_id', 'order_num']
#     assert all(col in recomms.columns for col in required_cols)
#     assert len(recomms) == 2
#     assert list(recomms['title_id'].values) == [1, 1]
#     assert list(recomms['suggested_title_id'].values) == ['/title/tt1345836/',
#                                                           '/title/tt1375666/']
#     assert list(recomms['order_num'].values) == [1, 2]