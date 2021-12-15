import os
from recsys.utils import parse_config
from recsys.core.etl import RawDetailsTransformer, RawReviewsTransformer

CONFIG_FILE = 'config.yaml'

def main():
    config = parse_config(CONFIG_FILE)
    


if __name__ == '__main__':
