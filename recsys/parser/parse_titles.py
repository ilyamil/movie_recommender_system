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


if __name__ == '__main__':
    titles = get_popular_titles(args.genres, args.n_titles)
    dump_obj(titles, args.titles_path)
