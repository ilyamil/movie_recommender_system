import sys
import pandas as pd
from pathlib import Path
from typing import List, Dict
from sklearn.metrics.pairwise import cosine_similarity

ROOT_FOLDER = Path(__file__).resolve().parents[3].as_posix()
sys.path.append(ROOT_FOLDER)

from recsys.core.models.base_recommender import BaseRecommender # noqa
from recsys.core.feature_extraction.base_feature_extractor import BaseFeatureExtractor # noqa


class ItemKNNRecommender(BaseRecommender):
    def __init__(self, metadata: pd.DataFrame, top_n: int, feature_manager):
        BaseRecommender.__init__(self, metadata, top_n)
        features = feature_manager.extract_features(metadata)
        self.features_mat = pd.DataFrame(
            cosine_similarity(features),
            index=metadata.index,
            columns=metadata.index
        )

    def _recommend(self, liked_movies: List[str]) -> Dict[str, float]:
        candidate_titles = []
        features_mat = self.features_mat[liked_movies].drop(liked_movies)
        for title in liked_movies:
            candidate_titles.append(features_mat[title].nlargest(self.top_n))
        recommendations = (
            pd.concat(candidate_titles)
            .groupby(level=0)
            .last()
            .nlargest(self.top_n)
        )
        return recommendations.to_dict()
