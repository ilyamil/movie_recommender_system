import os
from recsys.utils import parse_config
from recsys.core.data import CSVDataLoader
from recsys.core.etl import RawDetailsTransformer, RawReviewsTransformer


CONFIG_FILE = 'config.yaml'


def main():
    config = parse_config(CONFIG_FILE)
    reviews_src = config['etl']['reviews_src']
    reviews_trg = config['etl']['reviews_trg']
    details_src = config['etl']['details_src']
    details_trg = config['etl']['details_trg']

    reviews_dataloader = CSVDataLoader(reviews_src)
    

if __name__ == '__main__':
