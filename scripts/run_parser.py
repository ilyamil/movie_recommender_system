import argparse
from recsys.utils import parse_config
from recsys.imdb_parser.identifiers import IDCollector
# from recsys.imdb_parser.reviews import ReviewsCollector
# from recsys.imdb_parser.details import DetailsCollector
# from recsys.imdb_parser.other_reviews import OtherReviewsCollector


ATTRIBUTES = [
    'id',
    'details',
    'movie_reviews',
    'user_movie_reviews'
]
CONFIG_FILE = 'config.yaml'

parser = argparse.ArgumentParser()
parser.add_argument('-a', '--attribute', type=str,
                    help=(
                        f"""
                        Movie`s attribute to collect.
                        Possible attribues: {", ".join(ATTRIBUTES)}.
                        """
                        )
                    )
args = parser.parse_args()
config = parse_config(CONFIG_FILE, 'data_collection')


def run_parser(args, config):
    if args.attribute == 'id':
        collector = IDCollector(config['id'], config['logger'])
        collector.collect()
    elif args.attribute == 'details':
        # collector = DetailsCollector()
        # collector.collect_details()
        print(2)
    elif args.attribute == 'movie_reviews':
        # collector = ReviewsCollector()
        # collector.collect_reviews()
        print(3)
    elif args.attribute == 'user_movie_reviews':
        # collector = OtherReviewsCollector()
        # collector.collect_other_reviews()
        print(4)
    else:
        raise ValueError(
            f'possible values for --attribute: {", ".join(ATTRIBUTES)}'
        )


if __name__ == '__main__':
    run_parser(args, config)
