"""
Module contains functions to Extract, Transform, Load (ETL)
raw data collected by imdb parser.
"""
import ast
import re
from typing import Optional, Dict, List, Any
import pandas as pd


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


def correct_review_author(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize all review author identifiers in column 'author'
    by preserving only minimum valid part in form of '/user/urXXXXXX'.
    """
    if 'author' not in df_raw.columns:
        raise ValueError('No "author" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['author'] = ['author'].str.split('?', expand=True)[0]
    return df_


def cut_off_review_title_newline(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Removes '\n' at the end of each review title. 'title' column required.
    """
    if 'title' not in df_raw.columns:
        raise ValueError('No "title" column in input data')

    df_ = df_raw.copy(deep=False)
    df_['title'] = df_['title'].str.split('\n', expand=True)[0]
    return df_


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
    df_.loc[~df_['agg_rating'].str.contains('/10'), 'agg_rating'] = None
    df_[['rating', 'total_votes']] = (
        df_['agg_rating']
        .str.split('/10', expand=True)
        .values
    )
    df_['total_votes'] = df_['total_votes'].apply(expand_short_form)
    return (
        df_
        .astype({'rating': float, 'total_votes': float})
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


def split_with_capital_letter(x) -> Optional[List[str]]:
    """
    Splits a string into tokens by capital letter occurance.
    Example: 'ThisIsString' -> ['This', 'Is', 'String']
    """
    try:
        tokens = re.findall('[A-Z][^A-Z]*', x)
        entities = []
        entity = ''
        for token in tokens:
            stoken = token.strip()
            entity += ' ' + stoken if len(entity) != 0 else stoken
            if token[-1] != ' ':
                entities.append(entity)
                entity = ''
        return entities
    except Exception:
        return []


def extract_movie_details(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Extract the following movie details from column 'details'
    and add them to DataFrame:
        * 'release_date' (also convert to datetime),
        * 'country_of_origin',
        * 'production_company'
    After transformation the 'details' column is removed.
    """
    if 'details' not in df_raw.columns:
        raise ValueError('No "details" column in input data')

    df_ = df_raw.copy(deep=False)

    date_pattern = 'Release date(.+?)Countr'
    df_['release_date'] = (
        df_['details']
        .str.split(date_pattern, expand=True)[1]
        .str.split(' ', expand=True)
        .agg(lambda x: f'{x[0]} {x[1]} {x[2]}', axis=1)
    )
    df_['release_date'] = pd.to_datetime(df_['release_date'],
                                         format='%B %d, %Y',
                                         errors='coerce')

    country_pattern = '(Country of origin|Countries of origin)(.+?)Official'
    df_['country_of_origin'] = (
        df_['details']
        .str.split(country_pattern, expand=True)[2]
        .apply(lambda x: split_with_capital_letter(x))
    )

    company_pattern = '(Production companies|Production company)(.+?)See more'
    df_['production_company'] = (
        df_['details']
        .str.split(company_pattern, expand=True)[2]
        .apply(lambda x: split_with_capital_letter(x))
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


def convert_runtime_to_minutes(hours: str, minutes: str) -> Optional[int]:
    try:
        return int(hours) * 60 + int(minutes)
    except Exception:
        return None


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
        .apply(lambda x: convert_runtime_to_minutes(x[1], x[3]), axis=1)
    )
    return df_.drop('techspecs', axis=1)


def parse_actors(x) -> Dict[str, Any]:
    dct, title_id = x['actors'], x['title_id']
    records = {
        'title_id': [],
        'actor_id': [],
        'actor_name': [],
        'order_num': []
    }
    for name, ref in dct.items():
        records['title_id'].append(title_id)
        records['actor_id'].append(ref.split('?')[0] + '/')
        records['actor_name'].append(name)
        records['order_num'].append(int(ref.split('_')[-1]))
    return records


def parse_recommendations(x) -> Dict[str, Any]:
    recomms, title_id = x['imdb_recommendations'], x['title_id']
    records = {
        'title_id': [],
        'suggested_title_id': [],
        'order_num': []
    }
    for recomm in recomms:
        records['title_id'].append(title_id)
        records['suggested_title_id'].append(recomm.split('?')[0])
        records['order_num'].append(int(recomm.split('_')[-1]))
    return records


def parse_countries(x) -> Dict[str, Any]:
    countries, title_id = x['country_of_origin'], x['title_id']
    records = {
        'title_id': [],
        'country': [],
        'order_num': []
    }
    for num, country in enumerate(countries):
        records['title_id'].append(title_id)
        records['country'].append(country)
        records['order_num'].append(int(num + 1))
    return records


def parse_companies(x) -> Dict[str, Any]:
    companies, title_id = x['production_company'], x['title_id']
    records = {
        'title_id': [],
        'company': [],
        'order_num': []
    }
    for num, company in enumerate(companies):
        records['title_id'].append(title_id)
        records['company'].append(company)
        records['order_num'].append(int(num + 1))
    return records


def normalize(df_raw: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Create a dictionary with following keys:
    * 'main' - DataFrame with movie details (normalized columns excluded)
    * 'imdb_recommendations' - DataFrame with movie identifiers and
                               recommendations suggested by imdb for
                               these movies
    * 'actors' - DataFrame with movie identifiers and its actors played in
    * 'country_of_origin' - DataFrame with movie identifiers and its
                            country of origin
    * 'production_company' - DataFrame with movie identifiers and its
                             production companies
    After transformation the normalized columns is removed.
    All normalized DataFrames has an ID column - 'title_id'.
    """
    normalize_cols = [
        'imdb_recommendations',
        'actors',
        'country_of_origin',
        'production_company'
    ]
    if any(col not in df_raw.columns for col in normalize_cols):
        cols_fmt = '\n' + '\n'.join(normalize_cols)
        raise ValueError(
            f'There are no one or many of these cols: {cols_fmt}'
        )

    df_ = df_raw.copy(deep=False)

    df_['imdb_recommendations'] = (
        df_['imdb_recommendations'].apply(ast.literal_eval)
    )
    recomms = df_.apply(parse_recommendations, axis=1)
    recomms_df = pd.concat(
        (pd.DataFrame(x) for x in recomms.values),
        ignore_index=True
    )

    df_['actors'] = df_['actors'].apply(ast.literal_eval)
    actors = df_.apply(parse_actors, axis=1)
    actors_df = pd.concat(
        (pd.DataFrame(x) for x in actors.values),
        ignore_index=True
    )

    countries = df_.apply(parse_countries, axis=1)
    countries_df = pd.concat(
        (pd.DataFrame(x) for x in countries.values),
        ignore_index=True
    )

    companies = df_.apply(parse_companies, axis=1)
    companies_df = pd.concat(
        (pd.DataFrame(x) for x in companies.values),
        ignore_index=True
    )

    return {
        'main': df_.drop(columns=normalize_cols),
        'imdb_recommendations': recomms_df,
        'actors': actors_df,
        'country_of_origin': countries_df,
        'production_company': companies_df
    }
