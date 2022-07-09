import os
from argparse import ArgumentParser
from recsys.utils import parse_config
from recsys.imdb_parser.identifiers import IDCollector
from recsys.imdb_parser.metadata import MetadataCollector
# from recsys.imdb_parser.reviews import ReviewCollector
# from recsys.imdb_parser.details import DetailsCollector


ATTRIBUTES = [
    'id',
    'metadata',
    'reviews'
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
        collector = IDCollector(config['id'])
        collector.collect()
    elif arguments.attribute == 'metadata':
        collector = MetadataCollector(config['metadata'])
        collector.collect()
    # elif arguments.attribute == 'reviews':
    #     collector = ReviewCollector(config['data_collection']['reviews'],
    #                                 config['logger'])
    #     collector.collect()
    else:
        raise ValueError(
            f'possible values for --attribute: {", ".join(ATTRIBUTES)}'
        )


if __name__ == '__main__':
    main()
