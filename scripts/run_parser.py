import os
from argparse import ArgumentParser
from recsys.utils import parse_config
from recsys.imdb_parser.identifiers import IDCollector
from recsys.imdb_parser.reviews import ReviewCollector
from recsys.imdb_parser.details import DetailsCollector
# from recsys.imdb_parser.user_reviews import UserReviewsCollector


ATTRIBUTES = [
    'id',
    'details',
    'reviews',
    'user_reviews'
]
CONFIG_FILE = os.path.join('config', 'parser_config.yaml')


def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument(
        '-a', '--attribute', type=str,
        help=(
            f"""
            Movie`s attribute to collect.
            Possible attribues: {", ".join(ATTRIBUTES)}.
            """
            )
        )
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    config = parse_config(CONFIG_FILE)
    if arguments.attribute == 'id':
        collector = IDCollector(config['data_collection']['id'],
                                config['logger'])
        collector.collect()
    elif arguments.attribute == 'reviews':
        collector = ReviewCollector(config['data_collection']['reviews'],
                                    config['logger'])
        collector.collect()
    elif arguments.attribute == 'details':
        collector = DetailsCollector(config['data_collection']['details'],
                                     config['logger'])
        collector.collect()
    elif arguments.attribute == 'user_reviews':
        # collector = UserReviewsCollector()
        # collector.collect_other_reviews()
        print(4)
    else:
        raise ValueError(
            f'possible values for --attribute: {", ".join(ATTRIBUTES)}'
        )


if __name__ == '__main__':
    main()
