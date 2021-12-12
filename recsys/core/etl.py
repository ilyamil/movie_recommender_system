import ast
from typing import Optional, Dict, List
import pandas as pd
import numpy as np


def split_helpfulness_col(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split 'helpfulness' column of input dataframe into two
    distinct columns: 'upvotes' and 'total_votes'.
    After transformation 'helpfulness' column is removed.
    """
    if 'helpfulness' not in df_raw.columns:
        raise ValueError('No "helpfulness" column in input data')

    df_ = df_raw.copy(deep=False)
    df_[['upvotes', 'total_votes']] = (
        df_['helpfulness']
        .str.replace(',', '')
        .str.extractall('(\d+)')
        .unstack('match')
        .values
        .astype(int)
    )
    return df_.drop(columns=['helpfulness'])


def convert_to_date(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Convert 'date' column of type 'object' of input data to 'review_date'
    column of type datetime64[ns]. After transformation the 'date' column
    is removed.
    """
    if 'date' not in df_raw.columns:
        raise ValueError('No "date" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['review_date'] = pd.to_datetime(df_['date'])
    return df_.drop(columns=['date'])


def expand_short_form(string_num: str) -> Optional[float]:
    if string_num is None:
        return None

    short_forms = {
        'K': 1_000,
        'M': 1_000_000,
        'B': 1_000_000_000,
        'T': 1_000_000_000_000
    }
    last_char = string_num[-1]
    if last_char not in short_forms.keys():
        return float(string_num)
    return float(string_num[:-1]) * short_forms.get(last_char, None)


def split_aggregate_rating_col(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split column 'agg_rating' of type string into two columns:
    'movie_rating' of type float and 'movie_total_votes' of type int.
    After transformation the 'agg_rating' column is removed.
    """
    if 'agg_rating' not in df_raw.columns:
        raise ValueError('No "agg_rating" column in input data')

    df_ = df_raw.copy(deep=False)
    df_[['rating', 'total_votes']] = (
        df_['agg_rating']
        .str.split('/10', expand=True)
        .values
    )
    df_['total_votes'] = df_['total_votes'].apply(expand_short_form)
    return (
        df_
        .astype({'rating': np.float32, 'total_votes': np.int32})
        .drop('agg_rating', axis=1)
    )


def extract_original_title(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract title from 'original_title' column by splitting it.
    """
    if 'original_title' not in df_raw.columns:
        raise ValueError('No "original_title" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['original_title'] = (
        df_['original_title']
        .str.split('Original title: ',
                   expand=True)
        .iloc[:, 1]
    )
    return df_


def split_review_summary(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split column 'review_summary' into 3 columns: 'user_reviews_number',
    'critic_reviews_number', 'metascore'.
    After transformation the 'review_summary' columns is removed.
    """
    if 'review_summary' not in df_raw.columns:
        raise ValueError('No "review_summary" column in input data')

    df_ = df_raw.copy(deep=False)
    columns = [
        'user_reviews_num',
        'critic_reviews_num',
        'metascore'
    ]
    separator = 'User reviews|Critic reviews|Metascore'
    # convert string to pyhton dict in each row
    df_[columns] = pd.json_normalize(
        df_['review_summary']
        .apply(ast.literal_eval)
    )
    # get numbers associated with entities in 'columns'
    df_[columns] = (
        df_[columns]
        .apply(lambda x: x.str.split(separator))
        .apply(lambda x: x.str.get(0))
    )
    for col in columns:
        df_[col] = df_[col].apply(expand_short_form)

    return df_.drop('review_summary', axis=1)


def extract_tagline(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract tagline from column 'tagline'.
    """
    if 'tagline' not in df_raw.columns:
        raise ValueError('No "tagline" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['tagline'] = df_['tagline'].str.split('Taglines', expand=True)[1]
    return df_


def extract_substrings_after_anchors(s: str, anchors: List[str])\
        -> Optional[Dict[str, str]]:
    details = {}
    empty_anchors = []
    use_anchors = []
    for anchor in anchors:
        if anchor not in s:
            empty_anchors.append(anchor)
        else:
            use_anchors.append(anchor)
    for section_num in range(len(use_anchors)):
        start = use_anchors[section_num]
        left_loc = s.find(start)
        if section_num != len(use_anchors) - 1:
            end = use_anchors[section_num + 1]
            right_loc = s.rfind(end)
            details[start] = s[left_loc + len(start): right_loc]
        else:
            details[start] = s[left_loc + len(start):]
    details.update(**dict.fromkeys(empty_anchors))
    return details


def extract_movie_details(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the following movie details from column 'details'
    and add them to DataFrame:
        * 'release_date' (also convert to datetime),
        * 'country_of_origin',
        * 'also_known_as',
        * 'filming_locations',
        * 'production_companies'
    After transformation the 'details' column is removed.
    """
    if 'details' not in df_raw.columns:
        raise ValueError('No "details" column in input data')

    df_ = df_raw.copy(deep=False)
    details_sections = [
        'Release date',
        'Country of origin',
        'Official sites',
        'Languages',
        'Also known as',
        'Filming locations',
        'Production companies'
    ]
    df_[details_sections] = df_.apply(
        lambda x: extract_substrings_after_anchors(x, details_sections)
    )
    return df_.drop('details', axis=1)


def extract_boxoffice(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract 'budget' and 'boxoffice' information from column 'boxoffice'.
    These new columns are in a currency of origin and of type string with
    leading currency sign.
    """
    if 'boxoffice' not in df_raw.columns:
        raise ValueError('No "boxoffice" column in input data')

    df_ = df_raw.copy(deep=False)
    separator = 'Budget| |Gross worldwide|See detailed'
    df_[['budget', 'boxoffice']] = (
        df_['boxoffice']
        .str.split(separator, expand=True)
        .iloc[:, [1, 12]]
        .replace({'IMDbPro': None})
        .values
    )
    return df_


def extract_runtime(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract runtime information in a raw form from column 'techspecs'
    and add a column 'runtime'.
    After transformation the 'techspecs' column is removed.
    """
    if 'techspecs' not in df_raw.columns:
        raise ValueError('No "techspecs" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['runtime_min'] = (
        df_['techspecs']
        .str.split('Runtime| |Sound|Color', expand=True)
        .replace('', '0')
        .apply(lambda x: int(x[1]) * 60 + int(x[3]), axis=1)
    )
    return df_.drop('techspecs', axis=1)


def parse_actors_dict(x) -> Dict[str, Any]:
    dct, title_id = x['actors'], x['title_id']
    records = {
        'title_id': [],
        'actor_id': [],
        'actor_name': [],
        'order_num': []
    }
    for name, ref in dct.items():
        id_ = ref.split('?')[0] + '/'
        order_num = ref.split('_')[-1]
        records['title_id'].append(title_id)
        records['actor_id'].append(id_)
        records['actor_name'].append(name)
        records['order_num'].append(order_num)
    return records


def normalize_actors(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Create a new dataframe of movies actors from column 'actors'
    with the following columns:
        * 'title_id' - identifier of movie when an actor plays
        * 'actor_id' - identifier of the actor
        * 'actor_name' - name of the actor
        * 'order_num' - order number in cast list. Usually a main role
            assigns with order_num = 1, for second plan order_num = 2, etc.
    """
    if 'actors' not in df_raw.columns:
        raise ValueError('No "actors" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['actors'] = df_['actors'].apply(ast.literal_eval)
    records = df_.apply(parse_actors_dict, axis=1)
    return pd.concat((pd.DataFrame(x) for x in records.values),
                     ignore_index=True)
