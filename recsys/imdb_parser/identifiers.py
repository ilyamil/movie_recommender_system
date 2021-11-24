import re
from tqdm import tqdm
from typing import List, Dict, Any
from bs4 import BeautifulSoup
from recsys.utils import (dump_obj, get_full_path,
                          wait, get_response, create_logger)


GENRES = [
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
        self._genres = collector_config['genres']
        self._save_dir = collector_config['dir']
        self._n_titles = collector_config['n_titles']
        self._min_delay = collector_config['request_delay']['min_delay']
        self._max_delay = collector_config['request_delay']['max_delay']
        self._logger = create_logger(logger_config,
                                     collector_config['log_dir'])

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
                no_genre_msg = 'No valid genres were passed'
                self._logger.error(no_genre_msg)
                raise ValueError(no_genre_msg)
        else:
            self._genres = GENRES

    def _get_num_of_movies(self, page_content: bytes) -> int:
        page_html = BeautifulSoup(page_content, 'html.parser')
        tag_text = page_html.find('div', class_='desc')
        max_titles = re.search('of(.+?)title', tag_text.span.text)
        return int(max_titles.group(1).strip().replace(',', ''))

    def _get_movie_id(self, page_content: bytes) -> List[str]:
        page_html = BeautifulSoup(page_content, 'html.parser')
        titles_raw = page_html.find_all('h3', class_='lister-item-header')
        return [title.a['href'] for title in titles_raw]

    def _collect_rank_id(self, genre, rank) -> List[str]:
        url = URL_TEMPLATE.format(genre, rank)
        rank_id = []
        try:
            response = get_response(url)
            if response.status_code == 200:
                rank_id += self._get_movie_id(response.content)
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
        for rank in range(1, min(self._n_titles, max_titles), STEP):
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
                response = get_response(url)
                if response.status_code == 200:
                    max_titles = self._get_num_of_movies(response.content)
                    genre_id = self._collect_genre_id(genre, max_titles)

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
