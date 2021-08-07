import argparse
from recsys.utils import parse_config
from recsys.imdb_parser.identifiers import IDCollector
from recsys.imdb_parser.reviews import ReviewsCollector
from recsys.imdb_parser.details import DetailsCollector
from recsys.imdb_parser.other_reviews import OtherReviewsCollector


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


all_attributes = ['id', 'details', 'reviews', 'other_reviews']
parser = argparse.ArgumentParser()
parser.add_argument('-at', '--attribute', type=str,
                    help=(
                        'Movie`s attribute to collect. '
                        + f'Possible attributes: {", ".join(all_attributes)}'
                        )
                    )
args = parser.parse_args()

cfg = parse_config()['data_collection']

if __name__ == '__main__':
    movie_attribute = args.attribute
    if movie_attribute == 'id':
        attr_cfg = cfg['id_collection']
        version = attr_cfg['version']
        data_dir = attr_cfg['data_dir']
        genres = attr_cfg['genres']
        n_titles = attr_cfg['n_titles']
        min_delay = attr_cfg['request_delay']['min_delay']
        max_delay = attr_cfg['request_delay']['max_delay']

        use_genres = ALL_GENRES
        if genres.lower() != 'all':
            use_genres = set(genres).intersection(ALL_GENRES)
            genre_diff = set(genres) - set(use_genres)
            msg = f'No {", ".join(genre_diff)} in possible genres'
            print(msg)

        collector = IDCollector(use_genres, n_titles, min_delay, max_delay)
        collector.collect_id(data_dir, version)

    elif movie_attribute == 'details':
        collector = DetailsCollector()
        collector.collect_details()

    elif movie_attribute == 'reviews':
        collector = ReviewsCollector()
        collector.collect_reviews()

    elif movie_attribute == 'other_reviews':
        collector = OtherReviewsCollector()
        collector.collect_other_reviews()
    else:
        raise ValueError(
            f'--attribute must take the value in {", ".join(all_attributes)}'
            )
