from recsys.utils import parse_config
from recsys.core.handler import RequestHandler

CONFIG_FILE = 'config.yaml'


def main():
    config = parse_config(CONFIG_FILE)
    RequestHandler(config['inference'], config['logger'])


if __name__ == '__main__':
    main()
