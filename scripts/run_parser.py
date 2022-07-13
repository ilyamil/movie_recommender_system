import os
import time
from argparse import ArgumentParser
from recsys.utils import parse_config
from recsys.imdb_parser.identifiers import IDCollector
from recsys.imdb_parser.metadata import MetadataCollector
# from recsys.imdb_parser.reviews import ReviewCollector


ATTRIBUTES = [
    'id',
    'metadata',
    'reviews'
]
PARSER_CONFIG = os.path.join('config', 'parser_config.yaml')
CREDEENTIALS = os.path.join('config', 'credentials.yaml')
TIMEOUT = 10


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


def run_parser(arguments, config, credentials):
    if arguments.attribute == 'id':
        collector = IDCollector(config['id'], credentials['aws'])
        collector.collect()
    elif arguments.attribute == 'metadata':
        collector = MetadataCollector(config['metadata'], credentials['aws'])
        while not collector.is_all_metadata_collected():
            collector.collect()
            print(f'Timeout for {TIMEOUT} seconds\n')
            time.sleep(TIMEOUT)
    # elif arguments.attribute == 'reviews':
    #     collector = ReviewCollector(config['data_collection']['reviews'],
    #                                 config['logger'])
    #     collector.collect()
    else:
        raise ValueError(
            f'possible values for --attribute: {", ".join(ATTRIBUTES)}'
        )


if __name__ == '__main__':
    run_parser(
        parse_arguments(),
        parse_config(PARSER_CONFIG),
        parse_config(CREDEENTIALS)
    )
