import os
import re
from time import sleep
from typing import Dict, Any
from pathlib import Path
from tqdm import tqdm
from bs4 import BeautifulSoup
from pandas import DataFrame
from dotenv import load_dotenv
from recsys.utils import send_request, create_logger


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
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes Identifier Collector class. All parameters related to web
        scraping of movie identifiers must be specified in config.

        The config must contains the following fields:

        * mode: specifies where the results should be saved. When set up to
        'local' all movie related data will be saved on local machine, where
        application is running. When set up to 'cloud' related data saves on
        AWS. Using 'cloud' mode you also need to set up the following
        environment variables: AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY and
        AWS_S3_BUCKET.

        * metadata_file: name of file (possibly with folder) movies metadata
        will be saved to.

        * genres: list of genres you want to collect metadata about. It's also
        possible to set this field with 'all', in this case all available
        genres will be used. All possible genres can be found here
        https://www.imdb.com/feature/genre/?ref_=nv_ch_gr under
        "Popular Movies by Genre" title.

        * n_titles: number of titles to scrape information about in each genre.
        Set to null if want to use not absolute number of titles, but percent
        fraction. Titles in different genres could be overlapped.

        * pct_titles: percent of titles to scrape information about in each
        genre. Set to null if want to use absolute number of titles (parameter
        n_titles). Titles in different genres could be overlapped.

        * sleep_time: time in seconds a program will be wait for before going
        to next page. This parameter should be set reasonably, not too high (
        web scraping will last too long), not too low (increasing load on IMDB
        server for a long period of time is not ethical and such requests could
        be rate limited as a result).

        * log_file: file name to write logs related to collecting IDs.

        * log_level: minimal level of log messages.

        * log_msg_format: message format in logs.

        * log_dt_format: datetime format in logs.

        Notes:
        * One of these fields "n_titles" or "pct_titles" must be set to None,
            while the other set to desired value.
        """
        self._mode = config['mode']
        self._genres = config['genres']
        self._sleep_time = config['sleep_time']
        n_titles = config['n_titles']
        pct_titles = config['pct_titles']

        if self._mode == 'cloud':
            load_dotenv()
            self._storage_options = {
                'key': os.getenv('AWS_ACCESS_KEY'),
                'secret': os.getenv('AWS_SECRET_ACCESS_KEY')
            }
            if (not self._storage_options['key'])\
                    or (not self._storage_options['secret']):
                raise ValueError(
                    'AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY'
                    + ' must be specified in environment variables'
                )

            self._bucket = os.getenv('AWS_S3_BUCKET')
            if not self._bucket:
                raise ValueError(
                    'AWS_S3_BUCKET must be specified in environment variables'
                )

            self._metadata_file = os.path.join(
                's3://', self._bucket, config['metadata_file']
            )
        elif self._mode == 'local':
            self._storage_options = None

            root_dir = str(Path(__file__).parents[2])
            self._metadata_file = os.path.join(
                root_dir,
                'data',
                config['metadata_file']
            )
        else:
            raise ValueError('Supported modes: "local", "cloud"')

        self._logger = create_logger(
            filename=config['log_file'],
            msg_format=config['log_msg_format'],
            dt_format=config['log_dt_format'],
            level=config['log_level']
        )

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

        self._logger.info('Successfully initialized IDCollector')

    @staticmethod
    def collect_movie_id(soup: BeautifulSoup) -> Dict[str, Dict[str, str]]:
        title_raw = soup.find_all('div', {'class': 'lister-item-content'})
        title_id = [t.a['href'] for t in title_raw]
        main_genre = [
            extract_main_genre(t.find('span', {'class': 'genre'}).text)
            for t in title_raw
        ]
        return {t: {'main_genre': g} for t, g in zip(title_id, main_genre)}

    def _collect_rank_id(self, genre, rank) -> Dict[str, Dict[str, str]]:
        url = URL_TEMPLATE.format(genre, rank)
        rank_id = {}
        try:
            response = send_request(url)
            soup = BeautifulSoup(response.content, 'html.parser')
            if response.status_code == 200:
                old_len = len(rank_id)
                rank_id |= IDCollector.collect_movie_id(soup)
                self._logger.info(
                    f'Collected {len(rank_id) - old_len} new identifiers'
                    f' while parsing genre {genre.upper()},'
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

    def _collect_ids_for_genre(self, genre: str) -> Dict[str, Dict[str, str]]:
        genre_id = {}
        tqdm_params = {
            'iterable': range(1, self._sample_size[genre] + 1, STEP),
            'desc': genre,
            'unit_scale': STEP,
            'bar_format': BAR_FORMAT
        }
        for rank in tqdm(**tqdm_params):
            genre_id |= self._collect_rank_id(genre, rank)

            sleep(self._sleep_time)

        return genre_id

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on a disk or cloud.
        """
        print('Collecting identifiers...')

        id_genre = {}

        for genre in self._genres:
            old_len = len(id_genre)
            id_genre |= self._collect_ids_for_genre(genre)

            self._logger.info(
                f'Collected {len(id_genre) - old_len} new identifiers'
            )

            sleep(self._sleep_time)

        DataFrame.from_dict(id_genre, orient='index').to_json(
            self._metadata_file,
            storage_options=self._storage_options
        )


def extract_main_genre(s: str) -> str:
    return re.split(', | ', s.replace('\n', ''))[0]
