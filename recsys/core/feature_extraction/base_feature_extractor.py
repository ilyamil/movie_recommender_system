import pandas as pd
from abc import ABCMeta


class BaseFeatureExtractor(metaclass=ABCMeta):
    def extract_features(
        self,
        metadata: pd.DataFrame,
        **params
    ) -> pd.DataFrame:
        pass
