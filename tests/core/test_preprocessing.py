import pandas as pd
from recsys.core import preprocessing


def test_split_helpfulness_col():
    data = [
        {
            'id': 1,
            'helpfulness': '\n 171 out of 185 found this helpful.\n'
        },
        {
            'id': 2,
            'helpfulness': '\n 1,710 out of 1,850 found this helpful.\n'
        }
    ]
    df = pd.DataFrame(data)
    df_ = preprocessing.split_helpfulness_col(df)

    assert 'helpfulness' not in df_.columns
    assert ('upvotes' in df_.columns) and ('total_votes' in df_.columns)
    assert (df_.loc[0, 'upvotes'] == 171
            and df_.loc[0, 'total_votes'] == 185)
    assert (df_.loc[1, 'upvotes'] == 1710
            and df_.loc[1, 'total_votes'] == 1850)
