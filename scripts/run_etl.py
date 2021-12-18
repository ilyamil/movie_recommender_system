import os
from recsys.utils import parse_config, get_full_path
from recsys.core.data import CSVDataLoader
from recsys.core.etl import RawDetailsTransformer, RawReviewsTransformer


CONFIG_FILE = 'config.yaml'


def main():
    config = parse_config(CONFIG_FILE, 'etl')

    reviews_trg = get_full_path(config['reviews_trg'])
    details_trg = get_full_path(config['details_trg'])

    reviews_dataloader = CSVDataLoader(get_full_path(config['reviews_src']))
    details_dataloader = CSVDataLoader(get_full_path(config['details_src']))

    reviews_transformer = RawReviewsTransformer(reviews_dataloader)
    reviews = reviews_transformer.transform()
    reviews.to_parquet(os.path.join(reviews_trg, 'reviews.parquet'))

    details_transformer = RawDetailsTransformer(details_dataloader)
    details, actors, recomms = details_transformer.transform()
    details.to_parquet(os.path.join(details_trg, 'details.parquet'))
    actors.to_parquet(os.path.join(details_trg, 'actors.parquet'))
    recomms.to_parquet(os.path.join(details_trg, 'imdb_recomms.parquet'))


if __name__ == '__main__':
    main()
