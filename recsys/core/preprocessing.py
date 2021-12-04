import pandas as pd


def split_helpfulness_col(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Split 'helpfulness' column of input dataframe into two
    distinct columns: 'upvotes' and 'total_votes'.

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
    )
    return df_.drop(columns=['helpfulness'])
