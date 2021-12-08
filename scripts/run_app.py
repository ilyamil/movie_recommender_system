from argparse import ArgumentParser
from recsys.utils import parse_config
from recsys.core.handler import RequestHandler


CONFIG_FILE = 'config.yaml'


def parse_arguments():
    parser = ArgumentParser()
    parser.add_argument(
        '-m', '--movies', nargs='+',
        help=('Favorite movies you want to'
              ' get recommendations based on.')
    )
    args = parser.parse_args()
    return args


def main():
    arguments = parse_arguments()
    config = parse_config(CONFIG_FILE)
    handler = RequestHandler(config['inference'], config['logger'])
    print(handler.handle_request(arguments.movies))


if __name__ == '__main__':
    main()
