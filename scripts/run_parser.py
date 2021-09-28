import argparse
from recsys.utils import parse_config, create_logger
from recsys.imdb_parser.identifiers import IDCollector
# from recsys.imdb_parser.reviews import ReviewsCollector
# from recsys.imdb_parser.details import DetailsCollector
# from recsys.imdb_parser.other_reviews import OtherReviewsCollector


ALL_GENRES = [
    'documentary',
    'action',
    'adventure',
    'animation',
    'biography',
    'comedy',
    'crime',
    'drama',
    'family',
    'fantasy',
    'film_noir',
    'history',
    'horror',
    'music',
    'musical',
    'mystery',
    'romance',
    'sci_fi',
    'short',
    'sport',
    'thriller',
    'war',
    'western'
]
ALL_ATTRIBUTES = [
    'id',
    'details',
    'reviews',
    'other_reviews'
]
CONFIG_FILE = 'config.yaml'

parser = argparse.ArgumentParser()
parser.add_argument('-at', '--attribute', type=str,
                    help=(
                        'Movie`s attribute to collect. '
                        + f'Possible attributes: {", ".join(ALL_ATTRIBUTES)}'
                        )
                    )
args = parser.parse_args()
attribute = args.attribute


def run_parser(attribute):
    collection_cfg = parse_config(CONFIG_FILE)['data_collection']
    logger_cfg = parse_config(CONFIG_FILE)['logger']
    if attribute == 'id':
        save_dir = collection_cfg['id']['dir']
        genres = collection_cfg['id']['genres']
        n_titles = collection_cfg['id']['n_titles']
        min_delay = collection_cfg['id']['request_delay']['min_delay']
        max_delay = collection_cfg['id']['request_delay']['max_delay']
        log_dir = collection_cfg['id']['log_dir']
        logger = create_logger(logger_cfg, log_dir)

        if isinstance(genres, list):
            use_genres = set(genres).intersection(ALL_GENRES)
            genre_diff = set(genres) - set(use_genres)
            if genre_diff:
                logger.warning(
                    f'No {", ".join(genre_diff)} in possible genres'
                )
        elif isinstance(genres, str):
            if genres == 'all':
                use_genres = ALL_GENRES
            elif genres in ALL_GENRES:
                use_genres = genres
            else:
                msg = f'{genres} is not valid genre'
                logger.error(msg)
                raise ValueError(msg)
        else:
            msg = 'genres field in config file must be of type str or list'
            logger.error(msg)
            raise TypeError(msg)

        if use_genres:
            collector = IDCollector(use_genres, n_titles,
                                    save_dir, logger,
                                    min_delay, max_delay)
            collector.collect()
        else:
            msg = 'No valid genres were passed'
            logger.error(msg)
            raise ValueError(msg)
    elif attribute == 'details':
        # collector = DetailsCollector()
        # collector.collect_details()
        print(2)
    elif attribute == 'reviews':
        # collector = ReviewsCollector()
        # collector.collect_reviews()
        print(3)
    elif attribute == 'other_reviews':
        # collector = OtherReviewsCollector()
        # collector.collect_other_reviews()
        print(4)
    else:
        raise ValueError(
            f'possible values for --attribute: {", ".join(ALL_ATTRIBUTES)}'
        )


if __name__ == '__main__':
    run_parser(attribute)
