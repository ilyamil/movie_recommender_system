from typing import Tuple
from pandas import DataFrame
from recsys.core.data import AbstractDataLoader, AbstractDataTransformer
from recsys.core.pipeline import Pipeline
import recsys.etl.functions as etl_functions


class RawReviewsTransformer(AbstractDataTransformer):
    def __init__(self, dataloader: AbstractDataLoader):
        self._dataloader = dataloader

    def transform(self) -> DataFrame:
        raw_data = self._dataloader.load_data(True)
        pipeline = Pipeline(
            ('split helpfulness column', etl_functions.split_helpfulness_col),
            ('convert to datetime', etl_functions.convert_to_date)
        )
        return pipeline.compose(raw_data)


class RawDetailsTransformer(AbstractDataTransformer):
    def __init__(self, dataloader: AbstractDataLoader):
        self._dataloader = dataloader

    def transform(self) -> Tuple[DataFrame]:
        raw_details = self._dataloader.load_data(True)
        pipeline = Pipeline(
            ('split aggregate rating column',
                etl_functions.split_aggregate_rating_col),
            ('split review summary', etl_functions.split_review_summary),
            ('extract original title', etl_functions.extract_original_title),
            ('extract tagline', etl_functions.extract_tagline),
            ('extract details', etl_functions.extract_movie_details),
            ('extract boxoffice', etl_functions.extract_boxoffice),
            ('extract runtime', etl_functions.extract_runtime),
            ('table normalization', etl_functions.normalize)
        )
        details = pipeline.compose(raw_details)
        return details
