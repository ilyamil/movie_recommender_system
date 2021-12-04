import pandas as pd


def split_helpfulness_col(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split 'helpfulness' column of input dataframe into two
    distinct columns: 'upvotes' and 'total_votes'.
    After transformation 'helpfulness' column is removed.

    Args:
        df_raw (pd.DataFrame): input data with column.

    Returns:
        pd.DataFrame: Output data frame.
    """
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
    df_ = df_raw.copy(deep=False)
    df_['review_date'] = pd.to_datetime(df_['date'])
    return df_.drop(columns=['date'])