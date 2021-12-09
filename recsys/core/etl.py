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


def expand_short_form(string_num: str) -> int:
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


details_sections = [
        'Release date',
        'Country of origin',
        'Official site',
        'Languages',
        'Also known as',
        'Filming locations',
        'Production companies'
]


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


