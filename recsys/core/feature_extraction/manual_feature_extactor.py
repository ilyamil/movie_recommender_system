# Module contains code to extract hand-crafted features from movie metadata
import pandas as pd
from typing import List
from sklearn.preprocessing import OneHotEncoder
from .base_feature_extractor import BaseFeatureExtractor


def find_boxoffice_quantile_range(x, q):
    for i, j, l, r in zip(q, q[1:], q.index, q.index[1:]):
        if i <= x < j:
            return f'boxoffice_from_q{l}_to_q{r}'
    return 'boxoffice_unknown_q'


def concat_genres(row):
    genre = [row['genre_1']]
    if row['genre_2']:
        genre += [row['genre_2']]
    if row['genre_3']:
        genre += [row['genre_3']]
    return ', '.join(sorted(genre))


def get_runtime_bin_labels(bins: List[float]) -> List[str]:
    return [
        f'runtime_from_q{100*left:.0f}_to_q{100*right:.0f}'
        for left, right in zip(bins, bins[1:])
    ]


def replicate_n_times(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """
    Returns a copy of input DataFrame where each column replicated n times.
    """
    if n == 1:
        return df

    df_ = df.copy(deep=True)
    for col in df_.columns:
        for repl in range(1, n + 1):
            df_[col + f'copy_{repl}'] = df_[col].copy(deep=True)
    return df_


class ManualFeatureExtractor(BaseFeatureExtractor):
    def __init__(
        self,
        n_actor_feat: int,
        actor_feat_weight: int,
        n_genre_feat: int,
        genre_feat_weight: int,
        n_prod_comp_feat: int,
        prod_comp_feat_weight: int,
        n_country_feat: int,
        country_feat_weight: int,
        n_lang_feat: int,
        lang_feat_weight: int,
        boxoffice_quantiles: List[float],
        boxoffice_feat_weight: int,
        runtime_quantiles: List[float],
        runtime_feat_weight: int,
        rating_bins: List[int],
        rating_feat_weight: int,
        release_dt_feat_weight: int
    ):
        self.n_actor_feat = n_actor_feat
        self.n_genre_feat = n_genre_feat
        self.n_prod_comp_feat = n_prod_comp_feat
        self.n_country_feat = n_country_feat
        self.n_lang_feat = n_lang_feat
        self.boxoffice_quantiles = boxoffice_quantiles
        self.runtime_quantiles = runtime_quantiles
        self.rating_bins = rating_bins
        self.actor_feat_weight = actor_feat_weight
        self.genre_feat_weight = genre_feat_weight
        self.prod_comp_feat_weight = prod_comp_feat_weight
        self.country_feat_weight = country_feat_weight
        self.lang_feat_weight = lang_feat_weight
        self.boxoffice_feat_weight = boxoffice_feat_weight
        self.runtime_feat_weight = runtime_feat_weight
        self.rating_feat_weight = rating_feat_weight
        self.release_dt_feat_weight = release_dt_feat_weight

    @staticmethod
    def get_genre_features(
        metadata: pd.DataFrame,
        n_categories: int,
        weight: int
    ) -> pd.DataFrame:
        genre_comb = (
            metadata[['genre_1', 'genre_2', 'genre_3']]
            .apply(concat_genres, axis=1)
            .to_frame()
        )
        encoder = OneHotEncoder(max_categories=n_categories)
        encoder.fit(genre_comb)
        features = pd.DataFrame(
            data=encoder.transform(genre_comb).todense(),
            index=metadata.index,
            columns=encoder.get_feature_names()
        )
        features.columns = features.columns.str.removeprefix('x0_')
        return replicate_n_times(features, weight)

    @staticmethod
    def get_release_decade_features(
        metadata: pd.DataFrame,
        weight: int
    ) -> pd.DataFrame:
        release_decade = (
            metadata['release_date']
            .dt.year.round(-1)
            .to_frame()
        )
        encoder = OneHotEncoder()
        encoder.fit(release_decade)
        features = pd.DataFrame(
            data=encoder.transform(release_decade).todense(),
            index=metadata.index,
            columns=encoder.get_feature_names()
        )
        features.columns = features.columns.str.removeprefix('x0_')
        features.columns = features.columns.str[:-2]
        features = features.add_prefix('released_in_').add_suffix('s')
        features.columns.values[-1] = 'unknown_release_decade'
        return replicate_n_times(features.astype('int8'), weight)

    @staticmethod
    def get_actor_features(
        metadata: pd.DataFrame,
        n_categories: int,
        weight: int
    ) -> pd.DataFrame:
        top_n = 3
        actors = [
            (list(item.values()) + ['']*(top_n - len(item)))[:top_n]
            for item in metadata['actors']
        ]
        actors = pd.DataFrame.from_records(actors, index=metadata.index)

        encoder = OneHotEncoder(max_categories=n_categories)

        features = []
        for i in range(top_n):
            encoder.fit(actors[i].to_frame())
            feat = pd.DataFrame(
                data=encoder.transform(actors[i].to_frame()).todense(),
                index=metadata.index,
                columns=encoder.get_feature_names()
            ).drop('x0_infrequent_sklearn', axis=1)
            features.append(feat)

        features = pd.concat(features, axis=1)
        features = features[features.columns.unique()]
        features['another'] = features.sum(axis=1) == 0
        features.columns = features.columns.str.removeprefix('x0_/name/')
        features = (
            features
            .astype('int8')
            .add_prefix('actor_')
            .drop(columns='actor_x0_', errors='ignore')
        )
        return replicate_n_times(features, weight)

    @staticmethod
    def get_country_features(
        metadata: pd.DataFrame,
        n_categories: int,
        weight: int
    ) -> pd.DataFrame:
        country = metadata['country_of_origin_1'].to_frame()
        encoder = OneHotEncoder(max_categories=n_categories)
        encoder.fit(country)
        features = pd.DataFrame(
            data=encoder.transform(country).todense(),
            index=metadata.index,
            columns=encoder.get_feature_names()
        )
        features.columns = features.columns.str.removeprefix('x0_')
        features.columns.values[-1] = 'infrequent_country'
        features = features.add_prefix('originated_in_').astype('int8')
        return replicate_n_times(features, weight)

    @staticmethod
    def get_language_features(
        metadata: pd.DataFrame,
        n_categories: int,
        weight: int
    ) -> pd.DataFrame:
        language = metadata['original_language'].to_frame()
        encoder = OneHotEncoder(max_categories=n_categories)
        encoder.fit(language)
        features = pd.DataFrame(
            data=encoder.transform(language).todense(),
            index=metadata.index,
            columns=encoder.get_feature_names()
        )
        features.columns = features.columns.str.removeprefix('x0_')
        features.columns.values[-1] = 'infrequent'
        features = features.add_suffix('_language').astype('int8')
        return replicate_n_times(features, weight)

    @staticmethod
    def get_rating_features(
        metadata: pd.DataFrame,
        bins: List[int],
        weight: int
    ) -> pd.DataFrame:
        rating_cat_mapping = {
            f'({left}, {right}]': f'rating_from_{left}_to_{right}'
            for left, right in zip(bins, bins[1:])
        }
        rating = (
            pd.cut(metadata['rating'], bins)
            .astype(str)
            .apply(lambda x: rating_cat_mapping.get(x))
            .to_frame()
        )
        encoder = OneHotEncoder()
        encoder.fit(rating)
        features = pd.DataFrame(
            data=encoder.transform(rating).todense(),
            index=metadata.index,
            columns=encoder.get_feature_names()
        )
        features.columns = features.columns.str.removeprefix('x0_')
        return replicate_n_times(features.astype('int8'), weight)

    @staticmethod
    def get_prod_company_features(
        metadata: pd.DataFrame,
        n_categories: int,
        weight: int
    ) -> pd.DataFrame:
        company = metadata['production_company_1'].to_frame()
        encoder = OneHotEncoder(max_categories=n_categories)
        encoder.fit(company)
        features = pd.DataFrame(
            data=encoder.transform(company).todense(),
            index=metadata.index,
            columns=encoder.get_feature_names()
        )
        features.columns = features.columns.str.removeprefix('x0_')
        features.columns.values[-1] = 'infrequent_company'
        if 'None' in features.columns:
            features = features.rename({'None': 'unknown_company'}, axis=1)
        features = features.add_prefix('produced_by_').astype('int8')
        return replicate_n_times(features)

    @staticmethod
    def get_boxoffice_features(
        metadata: pd.DataFrame,
        quantiles: List[float],
        weight: int
    ) -> pd.DataFrame:
        dollars = metadata[metadata['boxoffice_gross_worldwide'].str[0] == '$']
        dollars['decade'] = dollars['release_date'].dt.year.round(-1)
        dollars['boxoffice'] = (
            dollars['boxoffice_gross_worldwide']
            .str.replace('$', '')
            .str.replace(',', '')
            .astype('int')
        )
        dollars_pivot = (
            dollars
            .groupby('decade')
            ['boxoffice']
            .quantile(quantiles, interpolation='nearest')
            .reset_index()
            .assign(quantile=lambda x: (100*x['level_1']).astype(int))
            .pivot(index='decade', columns='quantile', values='boxoffice')
        )
        dollars_pivot[100] += 1

        boxoffice_cat = (
            dollars[['boxoffice', 'decade']]
            .apply(lambda x: find_boxoffice_quantile_range(
                x['boxoffice'], dollars_pivot.loc[x['decade']]
            ), axis=1)
        )
        boxoffice_cat.name = 'boxoffice_category'
        unknown_boxoffice_cat = (
            metadata[~metadata.index.isin(boxoffice_cat.index)]
            .assign(boxoffice_category='boxoffice_unknown_q')
            ['boxoffice_category']
        )
        boxoffice = pd.concat([boxoffice_cat, unknown_boxoffice_cat])
        return replicate_n_times(pd.get_dummies(boxoffice), weight)

    @staticmethod
    def get_runtime_features(
        metadata: pd.DataFrame,
        quantiles: List[float],
        weight: int
    ) -> pd.DataFrame:
        labels = get_runtime_bin_labels(quantiles)
        runtime_features = pd.qcut(
            metadata['runtime'],
            quantiles,
            labels=labels
        )
        return replicate_n_times(pd.get_dummies(runtime_features), weight)

    def extract_features(self, metadata: pd.DataFrame) -> pd.DataFrame:
        metadata_ = metadata.copy(deep=False)
        features = pd.concat([
            self.get_boxoffice_features(
                metadata_,
                self.boxoffice_quantiles,
                self.boxoffice_feat_weight
            ),
            self.get_actor_features(
                metadata_,
                self.n_actor_feat,
                self.actor_feat_weight
            ),
            self.get_country_features(
                metadata_,
                self.n_country_feat,
                self.country_feat_weight
            ),
            self.get_genre_features(
                metadata_,
                self.n_genre_feat,
                self.genre_feat_weight
            ),
            self.get_language_features(
                metadata_,
                self.n_lang_feat,
                self.lang_feat_weight
            ),
            self.get_prod_company_features(
                metadata_,
                self.n_prod_comp_feat,
                self.prod_comp_feat_weight
            ),
            self.get_rating_features(
                metadata_,
                self.rating_bins,
                self.rating_feat_weight
            ),
            self.get_release_decade_features(
                metadata_,
                self.release_dt_feat_weight
            ),
            self.get_runtime_features(
                metadata_,
                self.runtime_quantiles,
                self.runtime_feat_weight
            )
        ], axis=1)
        return features.astype('uint8')
