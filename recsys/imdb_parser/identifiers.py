import re
import requests
from logging import Logger
from typing import Union, List
from bs4 import BeautifulSoup
from recsys.utils import dump_obj, get_filepath, wait


URL_TEMPLATE = (
    'https://www.imdb.com/search/title/?genres={}'
    + '&sort=num_votes,desc&start={}&explore=genres'
)
STEP = 50


class IDCollector:
    """
    Contains methods to load and parse web pages, then extract movie IDs.

    Public method:
        collect: parses web pages to extract ID
    """
    def __init__(self, version: int, genres: Union[List[str], str],
                 n_titles: int, save_dir: str, logger: Logger,
                 min_delay: int = 1, max_delay: int = 2):
        """
        Initializes collector class. All parameters related to collection
        of movie identifiers set up here.
        Using smaller min_delay and max_delay linearly speed up web scrapping
        but, on the other hand, it increases the workload on IMDB server which
        can lead to blocking of sending our requests and causing problems to
        IMDB. There should be a trade-off setting up these parameters.

        Args:
            version (int): version of parsing configuration. Each of the
                           configuration can differ from another by one
                           or many of following parameters
            genres (Union[List[str], str]): collect ID in these genres
            n_titles (int): number of movies to collect ID of in each genre
            save_dir (str): directory to save collected ID in certain genre
            logger (Logger): configured logger
            min_delay (int, optional): minimum time in seconds before next
                                       request to IMDB server. Defaults to 1.
            max_delay (int, optional): maximum time in seconds before next
                                       request to IMDB server. Defaults to 2.
        """
        self.genres = genres if isinstance(genres, list) else [genres]
        self.n_titles = n_titles
        self.save_dir = save_dir
        self.version = version
        self.logger = logger
        self.min_delay = min_delay
        self.max_delay = max_delay

    def _get_num_of_movies(self, page_content: bytes) -> int:
        page_html = BeautifulSoup(page_content, 'html.parser')
        tag_text = page_html.find('div', class_='desc')
        max_titles = re.search('of(.+?)title', tag_text.span.text)
        return int(max_titles.group(1).strip().replace(',', ''))

    def _get_movie_id(self, page_content: bytes) -> List[str]:
        page_html = BeautifulSoup(page_content, 'html.parser')
        titles_raw = page_html.find_all('h3', class_='lister-item-header')
        return [title.a['href'] for title in titles_raw]

    def _collect_genre_id(self, genre: str, max_titles: int) -> List[int]:
        genre_id = []
        for rank in range(1, min(self.n_titles, max_titles), STEP):
            url = URL_TEMPLATE.format(genre, rank)
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    rank_id = self._get_movie_id(response.content)
                    genre_id += rank_id
                    msg = (
                        f'Collected {len(rank_id)} ID '
                        + f'in genre {genre.upper()}, '
                        + f'rank {rank}-{rank + STEP}'
                    )
                    self.logger.info(msg)
                else:
                    msg = (
                        f'Bad status code in genre {genre.upper()},'
                        + f'rank {rank}-{rank + STEP}'
                    )
                    self.logger.warning(msg)
            except Exception as e:
                msg = (
                    f'Exception in genre {genre.upper()},'
                    + f'rank {rank}-{rank + STEP}'
                    + f'with message: {e}'
                )
                self.logger.warning(msg)

        wait(self.min_delay, self.max_delay)

        return genre_id

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on disk.
        """
        for genre in self.genres:
            url = URL_TEMPLATE.format(genre, 1)
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    max_titles = self._get_num_of_movies(response.content)
            except Exception as e:
                msg = (
                    f'Exception in finding num of movies with message: {e}.'
                    + f'Genre {genre.upper()} skipped'
                )
                self.logger.warning(msg)

            wait(self.min_delay, self.max_delay)

            genre_id = self._collect_genre_id(genre, max_titles)

            filename = f'{genre.upper()}__{len(genre_id)}_v{self.version}'
            filepath = get_filepath(self.save_dir, filename)
            dump_obj(genre_id, filepath)

            msg = f'Collected {len(genre_id)} ID in {genre.upper()} genre'
            self.logger.info(msg)
