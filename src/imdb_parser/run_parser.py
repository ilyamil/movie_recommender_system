import os
import argparse
import sys
print(sys.path)
from .. import utils
# from src.imdb_parser.movie_id_collection import MovieIDCollector
# from movie_reviews_collection import MovieReviewsCollector
# from movie_details_collection import MovieDetailsCollector
# from user_reviews_collection import UserReviewsCollector

ALL_GENRES = [
    'documentary',
    'action',
    'adventure',
    'animation',
    'biography',
    'comedy',
    'crime',
    'drama',
    'family',
    'fantasy',
    'film_noir',
    'history',
    'horror',
    'music',
    'musical',
    'mystery',
    'romance',
    'sci_fi',
    'short',
    'sport',
    'thriller',
    'war',
    'western'
]

all_attributes = ['id', 'details', 'reviews', 'user_other_reviews']
parser = argparse.ArgumentParser()
parser.add_argument('-at', '--attribute', type=str,
                    help=(
                        'Movie`s attribute to collect. '
                        + f'Possible attributes: {", ".join(all_attributes)}'
                        )
                    )
args = parser.parse_args()


if __name__ == '__main__':
    movie_attribute = args.attribute
    if movie_attribute == 'id':
        print(1)
    elif movie_attribute == 'details':
        print(2)
    elif movie_attribute == 'reviews':
        print(3)
    elif movie_attribute == 'user_other_reviews':
        print(4)
    else:
        raise ValueError(
            f'--attribute must take the value in {all_attributes}'
            )

