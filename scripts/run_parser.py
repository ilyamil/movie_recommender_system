import os
import time
from argparse import ArgumentParser
from recsys.utils import parse_config, check_health_status
from recsys.imdb_parser.identifiers import IDCollector
from recsys.imdb_parser.metadata import MetadataCollector
from recsys.imdb_parser.reviews import ReviewCollector


ATTRIBUTES = [
    'id',
    'metadata',
    'reviews'
]
PARSER_CONFIG = os.path.join('config', 'parser_config.yaml')
CREDEENTIALS = os.path.join('config', 'credentials.yaml')
URL_FOR_HEALTH_CHECK = 'https://www.imdb.com/chart/top/?ref_=nv_mv_250'
TIMEOUT = 5


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
        if not check_health_status(URL_FOR_HEALTH_CHECK):
            print('Stop parsing. Recieved 4xx or 5xx status code')

        collector = IDCollector(config['id'], credentials['aws'])
        collector.collect()
    elif arguments.attribute == 'metadata':
        collector = MetadataCollector(config['metadata'], credentials['aws'])
        while not collector.is_all_metadata_collected():
            if not check_health_status(URL_FOR_HEALTH_CHECK):
                print('Stop parsing. Recieved 4xx or 5xx status code')
                break

            collector.collect()

            print(f'Timeout for {TIMEOUT} seconds\n')
            time.sleep(TIMEOUT)
    elif arguments.attribute == 'reviews':
        import psutil
        collector = ReviewCollector(config['reviews'], credentials['aws'])
        print('Initial memory usage: ',
                psutil.Process().memory_info().rss / (1024 * 1024))
        i = 0
        while not collector.is_all_reviews_collected():
            if not check_health_status(URL_FOR_HEALTH_CHECK):
                print('Stop parsing. Recieved 4xx or 5xx status code')
                break

            _ = collector.collect()
            print(f'Memory usage on {i}-th iteration',
                    psutil.Process().memory_info().rss / (1024 * 1024))
            i += 1
            print(f'Timeout for {TIMEOUT} seconds\n')
            time.sleep(TIMEOUT)

            collector = ReviewCollector(config['reviews'], credentials['aws'])
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
