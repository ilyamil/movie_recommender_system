import re
from tqdm import tqdm
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from recsys.utils import (dump_obj, get_full_path,
                          wait, send_request, create_logger)

MOVIE_COUNT_BY_GENRE = {
    'action': 49210,
    'adventure': 24075,
    'animation': 7479,
    'biography': 7519,
    'comedy': 98571,
    'crime': 33263,
    'documentary': 809072,
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
    'short': 1063288,
    'sport': 4824,
    'thriller': 49018,
    'war': 9499,
    'western': 8880
}
GENRES = list(MOVIE_COUNT_BY_GENRE.keys())
URL_TEMPLATE = (
    'https://www.imdb.com/search/title/?genres={}'
    '&sort=num_votes,desc&start={}&explore=genres'
)
STEP = 50


class IDCollector:
    """
    Contains methods to load and parse web pages, then extract movie IDs.

    Public method:
        collect: parses web pages to extract and save ID on disk.
    """
    def __init__(self, collector_config: Dict[str, Any],
                 logger_config: Dict[str, Any]) -> None:
        """
        Initializes collector class. All parameters related to collection
        of movie identifiers set up here using config.
        Using smaller min_delay and max_delay linearly speed up web scrapping
        but, on the other hand, it increases the workload on IMDB server which
        can lead to blocking of sending our requests and causing problems to
        IMDB. There should be a trade-off setting up these parameters.
        """
        self._logger = create_logger(logger_config,
                                     collector_config['log_dir'])
        self._save_dir = collector_config['dir']
        self._min_delay = collector_config['request_delay']['min_delay']
        self._max_delay = collector_config['request_delay']['max_delay']

        self._genres = collector_config['genres']
        if not isinstance(self._genres, list):
            self._genres = [self._genres]
        self._genres = [genre.lower() for genre in self._genres]
        if 'all' not in self._genres:
            use_genres = set(self._genres).intersection(GENRES)
            genre_diff = set(self._genres) - set(use_genres)
            if genre_diff:
                self._logger.warning(
                    f'No {", ".join(genre_diff)} in possible genres'
                )
            if not use_genres:
                no_genre_msg = 'No valid genres were passed'
                self._logger.error(no_genre_msg)
                raise ValueError(no_genre_msg)
            self._genres = use_genres
        else:
            self._genres = GENRES

        n_titles = collector_config['n_titles']
        pct_titles = collector_config['pct_titles']
        if n_titles and pct_titles:
            raise ValueError(
                'Only one of these arguments needs to be set in config file:'
                'n_titles or pct_titles'
            )
        if not n_titles and not pct_titles:
            raise ValueError(
                'One of these arguments needs to be set in config file:'
                'n_titles or pct_titles'
            )
        if pct_titles and not 0 < pct_titles < 100:
            raise ValueError('pct_titles must lie in the interval [0, 100]')

        if pct_titles:
            self._sample_size = {
                genre: int(pct_titles * MOVIE_COUNT_BY_GENRE[genre])
                for genre in self._genres
            }
        else:
            self._sample_size = {
                genre: min(n_titles, MOVIE_COUNT_BY_GENRE[genre])
                for genre in self._genres
            }

    @staticmethod
    def get_movies_cnt(page_content: bytes) -> int:
        page_html = BeautifulSoup(page_content, 'html.parser')
        tag_text = page_html.find('div', class_='desc')
        max_titles = re.search('of(.+?)title', tag_text.span.text)
        return int(max_titles.group(1).strip().replace(',', ''))

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

    def _collect_genre_id(self, genre: str, max_titles: int) -> List[int]:
        genre_id = []
        for rank in range(1, min(self._sample_size[genre], max_titles), STEP):
            genre_id += self._collect_rank_id(genre, rank)
            wait(self._min_delay, self._max_delay)
        return genre_id

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on disk.
        """
        for genre in tqdm(self._genres):
            url = URL_TEMPLATE.format(genre, 1)
            try:
                response = send_request(url)
                if response.status_code == 200:
                    max_titles = IDCollector.get_movies_cnt(response.content)
                    genre_id = IDCollector.collect_genre_id(genre, max_titles)

                    filename = f'{genre.upper()}__{len(genre_id)}'
                    filepath = get_full_path(self._save_dir, filename)
                    dump_obj(genre_id, filepath)

                    wait(self._min_delay, self._max_delay)

                    self._logger.info(
                        f'Collected {len(genre_id)} identifiers'
                        f' in {genre.upper()} genre in total'
                    )
                else:
                    raise Exception('Bad status code')
            except Exception as e:
                self._logger.warning(
                    f'Exception in finding num of movies with message: {e}.'
                    f' Genre {genre.upper()} skipped'
                )
