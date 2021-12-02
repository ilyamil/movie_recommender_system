from typing import List, Dict, Any
from tqdm import tqdm
from bs4 import BeautifulSoup
from recsys.utils import (dump_obj, get_full_path,
                          wait, send_request, create_logger)


BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'
# total number of movies collected manually as of December 2021
MOVIE_COUNT_BY_GENRE = {
    'action': 49210,
    'adventure': 24075,
    'animation': 7479,
    'biography': 7519,
    'comedy': 98571,
    'crime': 33263,
    'drama': 209137,
    'family': 15800,
    'fantasy': 15831,
    'film-noir': 818,
    'history': 8345,
    'horror': 33292,
    'music': 6883,
    'musical': 10226,
    'mystery': 17137,
    'romance': 48572,
    'sci-fi': 15326,
    'sport': 4824,
    'thriller': 49018,
    'war': 9499,
    'western': 8880
}
GENRES = list(MOVIE_COUNT_BY_GENRE.keys())
URL_TEMPLATE = (
    'https://www.imdb.com/search/title/?title_type=feature&genres={}'
    '&sort=num_votes,desc&start={}&explore=genres&ref_=adv_nxt'
)
STEP = 50


class IDCollector:
    """
    Contains methods for parsing IMDB movie search web pages,
    then extract movie identifiers from them.

    Public method:
        collect: parses pages and saves IDs on a disk.
    """
    def __init__(self, collector_config: Dict[str, Any],
                 logger_config: Dict[str, Any]) -> None:
        """
        Initializes collector class. All parameters related to collection
        of movie identifiers set up here using config.
        The following fields must be set in config file:
            1. dir - a directory to save identifiers.
            2. log_file - a file to write all logs while collecting IDs
            3. genres - genres of movies to collect.
                All possible genres can be found there
                https://www.imdb.com/feature/genre/?ref_=nv_ch_gr
                under "Popular Movies by Genre" title.
            4. n_titles - number of movies' identifiers to collect in
                a specified genre.
            5. pct_titles - percent of total movies available to collect
                in a specified genre.
            6. request_delay: min_delay and max_delay - lower and upper bound
                of waiting time before next bunch of identifiers will
                be processed.
        Notes:
        * One of these fields "n_titles" or "pct_titles" must be set to None,
            while the other set to desired value.
        * Using smaller min_delay and max_delay linearly speed up web
            scrapping but, on the other hand, it increases the workload
            on IMDB server which is not totally ethical and could lead to
            blocking of our requests. This trade-off is up to you.
        """
        log_file = collector_config['log_file']
        self._logger = create_logger(logger_config, log_file)
        self._save_dir = collector_config['dir']
        self._min_delay = collector_config['request_delay']['min_delay']
        self._max_delay = collector_config['request_delay']['max_delay']
        self._genres = collector_config['genres']
        n_titles = collector_config['n_titles']
        pct_titles = collector_config['pct_titles']

        if not isinstance(self._genres, list):
            self._genres = [self._genres]

        if 'all' not in self._genres:
            use_genres = set(self._genres).intersection(GENRES)
            genre_diff = set(self._genres) - set(use_genres)
            if genre_diff:
                self._logger.warning(
                    f'No {", ".join(genre_diff)} in possible genres'
                )
            if not use_genres:
                raise ValueError('No valid genres were passed')
            self._genres = use_genres
        else:
            self._genres = GENRES

        if not (n_titles or pct_titles):
            raise ValueError(
                'Only one of these arguments needs to be set'
                ' in config file: n_titles or pct_titles'
            )
        if pct_titles:
            if not 0 <= pct_titles <= 100:
                raise ValueError(
                    'pct_titles must lie in the interval [0, 100]'
                )
            self._sample_size = {
                genre: int(pct_titles / 100 * MOVIE_COUNT_BY_GENRE[genre])
                for genre in self._genres
            }
        else:
            self._sample_size = {
                genre: min(n_titles, MOVIE_COUNT_BY_GENRE[genre])
                for genre in self._genres
            }

    @staticmethod
    def collect_movie_id(page_content: bytes) -> List[str]:
        page_html = BeautifulSoup(page_content, 'html.parser')
        titles_raw = page_html.find_all('h3', class_='lister-item-header')
        return [title.a['href'] for title in titles_raw]

    def _collect_rank_id(self, genre, rank) -> List[str]:
        url = URL_TEMPLATE.format(genre, rank)
        rank_id = []
        try:
            response = send_request(url)
            if response.status_code == 200:
                rank_id += IDCollector.collect_movie_id(response.content)
                self._logger.info(
                    f'Collected {len(rank_id)} identifiers'
                    f' in genre {genre.upper()},'
                    f' rank {rank}-{rank + STEP}'
                )
            else:
                self._logger.warning(
                    f'Bad status code in genre {genre.upper()},'
                    f' rank {rank}-{rank + STEP}'
                )
        except Exception as e:
            self._logger.warning(
                f'Exception in genre {genre.upper()},'
                f' rank {rank}-{rank + STEP}'
                f' with message: {e}'
            )
        finally:
            return rank_id

    def _collect_genre_id(self, genre: str) -> List[int]:
        genre_id = []
        tqdm_params = {
            'iterable': range(1, self._sample_size[genre] + 1, STEP),
            'desc': genre,
            'unit_scale': STEP,
            'bar_format': BAR_FORMAT
        }
        for rank in tqdm(**tqdm_params):
            genre_id += self._collect_rank_id(genre, rank)

            wait(self._min_delay, self._max_delay)

        return genre_id

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on a disk.
        """
        print('Collecting identifiers...')
        for genre in self._genres:
            genre_id = self._collect_genre_id(genre)

            filepath = get_full_path(self._save_dir, f'{genre}.pkl')
            dump_obj(genre_id, filepath)

            wait(self._min_delay, self._max_delay)

            self._logger.info(
                    f'Collected {len(genre_id)} identifiers'
                    f' in {genre.upper()} genre in total'
            )
