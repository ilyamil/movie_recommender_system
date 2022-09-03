# This module contains simple models, which predicts using simple heiuristics
# like most popular movies or movies recommended by IMDB
import pandas as pd
from typing import Dict, List
from itertools import chain
from .base_recommender import BaseRecommender


class PopularRecommender(BaseRecommender):
    def __init__(self, metadata: pd.DataFrame, top_n: int):
        BaseRecommender.__init__(self, metadata)
        self.popular_titles = (
            self.metadata
            .query('num_votes > 50000')
            .nlargest(top_n, 'rating')
        )

    def _recommend(self) -> Dict[str, float]:
        return self.popular_titles['rating'].to_dict()


class IMDBRecommender(BaseRecommender):
    def __init__(self, metadata: pd.DataFrame, top_n: int):
        BaseRecommender.__init__(self, metadata)
        self.metadata['imdb_recommendations_set'] = (
            self.metadata['imdb_recommendations']
            .apply(lambda x: set(x.values()))
        )
        self.top_n = top_n

    def _recommend(self, user_liked_movies: List[str]) -> Dict[str, float]:
        # metadata about movies that user liked
        user_liked_movies_metadata = self.metadata[
            self.metadata.index.isin(user_liked_movies)
        ]
        # set of recommendations for each liked movie
        all_imdb_recommendations = (
            user_liked_movies_metadata['imdb_recommendations_set']
            .tolist()
        )
        # metadata for unique recommendations for all liked movies
        # recommend only movies with highest rating
        unique_recommendations_metadata = self.metadata[
            self.metadata.index.isin(set(chain(*all_imdb_recommendations)))
        ]
        return (
            unique_recommendations_metadata
            .nlargest(self.top_n, 'rating')
            ['rating']
            .to_dict()
        )
