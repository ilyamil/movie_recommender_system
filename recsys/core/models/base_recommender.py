from abc import ABCMeta
from typing import List, Dict, Any
import pandas as pd


class BaseRecommender(metaclass=ABCMeta):
    RELEVANT_METADATA = [
        'original_title',
        'genre',
        'release_date',
        'country_of_origin_1',
        'production_company_1',
        'rating',
        'poster_url'
    ]

    def __init__(self, metadata: pd.DataFrame, **model_params):
        self.metadata = metadata

    def _recommend(self, **user_input) -> Dict[str, float]:
        pass

    def _format_recommendations(
        self,
        identifiers: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Returns relevant metadata for each predicted movie.
        """
        metadata_ = self.metadata[self.metadata.index.isin(identifiers)]
        metadata_.loc[identifiers.keys(), 'relevance'] = list(
            identifiers.values()
        )
        metadata_['genre'] = (
            metadata_[['genre_1', 'genre_2', 'genre_3']]
            .apply(lambda x: ', '.join(x.dropna()), axis=1)
        )
        metadata_['release_date'] = (
            metadata_['release_date']
            .dt.strftime('%B %Y')
        )
        metadata_['rating'] = metadata_['rating'].round(1)

        return (
            metadata_
            .sort_values('relevance', ascending=False)
            [self.RELEVANT_METADATA]
            .to_dict(orient='records')
        )

    def recommend(
        self,
        user_liked_movies: List[str] = None,
        user_preferences: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        user_input = {
            'user_liked_movies': user_liked_movies,
            'user_preferences': user_preferences
        }
        raw_recommendations = self._recommend(**user_input)
        return self._format_recommendations(raw_recommendations)
