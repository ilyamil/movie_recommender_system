import os
import argparse
from src.review_extraction import get_reviews
from src.utils import load_obj


parser = argparse.ArgumentParser()
parser.add_argument('--titles_path', metavar='TP', type=str,
                    help='Path you want to save or already have title data')
parser.add_argument('--reviews_path', metavar='RP', type=str,
                    help='Path you want to save review data')
parser.add_argument('--starts_with', metavar='SW', type=int, default=0,
                    help='The ordinal number of title in "TITLES_PATH"'
                         + ' from which we start collecting data')
parser.add_argument('--chunk_size', metavar='CZ', type=int, default=100,
                    help='The number of titles we want to collect data about'
                         + ' during one session')
parser.add_argument('--lr', type=int, default=1,
                    help='Lowest rating of reviews')
parser.add_argument('--hr', type=int, default=10,
                    help='Highest rating of reviews')
args = parser.parse_args()


if __name__ == '__main__':
    titles = load_obj(args.titles_path)
    titles_part = titles[args.starts_with: args.starts_with + args.chunk_size]
    reviews = get_reviews(titles_part, args.lr, args.hr)
    if os.path.exists(args.reviews_path):
        reviews.to_csv(args.reviews_path, mode='a', header=False, index=False)
    else:
        reviews.to_csv(args.reviews_path, index=False)
