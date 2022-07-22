from ast import arg
import os
from argparse import ArgumentParser
from recsys.utils import parse_config
from recsys.imdb_parser.etl import ReviewsETL, MetadataETL


PARSER_CONFIG = os.path.join('config', 'parser_config.yaml')


def parse_arguments():
    parser = ArgumentParser(
        description='Python script for running ETL job.'
    )
    parser.add_argument(
        '-e',
        '--entity',
        type=str,
        choices=['metadata', 'reviews'],
        help='Review data or Metadata'
    )
    return parser.parse_args()


def main():
    arguments = parse_arguments()
    config = parse_config(PARSER_CONFIG)
    if arguments.entity == 'metadata':
        pass
    elif arguments.entity == 'reviews':
        ReviewsETL(config['etl']).run()


if __name__ == '__main__':
    main()
