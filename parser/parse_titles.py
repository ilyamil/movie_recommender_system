import argparse
from src.title_extraction import get_popular_titles
from src.utils import dump_obj

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

parser = argparse.ArgumentParser()
parser.add_argument('--titles_path', type=str,
                    help='Path you want to save or already have title data')
parser.add_argument('--genres', nargs='+', type=str, default='all',
                    help='Genres you want to collect reviews about')
parser.add_argument('--n_titles', type=int, default=50,
                    help='Number of titles in each genre')
args = parser.parse_args()


if __name__ == '__main__':
    titles = get_popular_titles(args.genres, args.n_titles)
    dump_obj(titles, args.titles_path)
