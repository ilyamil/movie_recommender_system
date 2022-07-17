import os
import re
from time import sleep
from pathlib import Path
from typing import Optional, Dict, List, Any, Union
from pandas import DataFrame, read_json
from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv
from recsys.utils import send_request, create_logger


BAR_FORMAT = '{percentage:3.0f}%|{bar:20}{r_bar}'
BASE_URL = 'https://www.imdb.com{}'
TOP_N_ACTORS = 10
BATCH_SIZE = 50


class MetadataCollector:
    """
    Contains methods for parsing IMDB movie search web pages,
    then extract movie identifiers from them.

    Public methods:
        * collect_title_details: parses web page of given movie.
        * is_all_metadata_collected: check if there any title we can scrape
        details about.
        * collect: parses pages and saves IDs on a disk or cloud.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initializes Metadata Collector class. All parameters related to web
        scraping of movie metadata must be specified in config.

        The config must contains the following fields:
        * mode: specifies where the results should be saved. When set up to
        'local' all movie related data will be saved on local machine, where
        application is running. When set up to 'cloud' related data saves on
        AWS. Using 'cloud' mode you also need to set up the following
        environment variables: AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY and
        AWS_S3_BUCKET.

        * metadata_file: name of file (possibly with folder) movies metadata
        will be saved to.

        * chunk_size: number of movies a program try to parse in one iteration.
        After each iteration there is a timout period to prevent too many
        requests.

        * sleep_time: time in seconds a program will be wait for before going
        to next movie. This parameter should be set reasonably, not too high
        (web scraping will last too long), not too low (increasing load on IMDB
        server for a long period of time is not ethical and such requests could
        be rate limited as a result).

        * log_file: file name to write logs related to collecting IDs.

        * log_level: minimal level of log messages.

        * log_msg_format: message format in logs.

        * log_dt_format: datetime format in logs.
        """
        self._mode = config['mode']
        self._chunk_size = config['chunk_size']
        self._sleep_time = config['sleep_time']

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

            self._root_dir = str(Path(__file__).parents[2])
            self._metadata_file = os.path.join(
                self._root_dir,
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

        self._logger.info('Successfully initialized MetadataCollector')

    @staticmethod
    def collect_title_details(soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Collects the following details (if exists) about a single movie:
            * original title
            * poster url
            * summary of reviews scores
            * average rating
            * actors starred in the movie
            * imdb recommendations to the movie
            * boxoffice
            * runtime
        """
        return {
            'original_title': collect_original_title(soup),
            'genres': collect_genres(soup),
            'poster_url': collect_poster_url(soup),
            'review_summary': collect_review_summary(soup),
            'agg_rating': collect_aggregate_rating(soup),
            'actors': collect_actors(soup),
            'imdb_recommendations': collect_imdb_recommendations(soup),
            'details': collect_details_summary(soup),
            'boxoffice': collect_boxoffice(soup)
        }

    def is_all_metadata_collected(self) -> bool:
        """
        Checks are there any movie in a database which metadata was not
        collected yet.
        """
        metadata_df = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        if 'genres' not in metadata_df.columns:
            already_collected = 0
        else:
            already_collected = (~metadata_df['genres'].isna()).sum()
        total_movies = len(metadata_df['genres'])

        print(
            f'Movie metadata is already collected for {already_collected}'
            + f' out of {total_movies} titles'
        )
        return total_movies == already_collected

    def collect(self) -> None:
        """
        Parses relevant web pages to extract movie identifiers and write
        them on a disk or cloud.
        """
        print('Collecting metadata...')

        movie_metadata_df = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        movie_metadata = movie_metadata_df.T.to_dict()

        title_ids = [t for t, _ in movie_metadata.items()]
        counter = 0
        session_counter = 0
        for i, title_id in tqdm(
            enumerate(title_ids), total=len(title_ids), bar_format=BAR_FORMAT
        ):
            if movie_metadata[title_id].get('original_title', None):
                continue

            url = BASE_URL.format(title_id)
            try:
                title_page = send_request(url)
                soup = BeautifulSoup(title_page.text, 'lxml')

                details = self.collect_title_details(soup)
                movie_metadata[title_id] |= details

                counter += 1

                self._logger.info(f'Collected metadata for title {title_id}')
            except Exception as e:
                self._logger.warn(f'Exception {str(e)} in parsing {url}')
            finally:
                sleep(self._sleep_time)

            # save results after if we have enough new data
            if (counter == BATCH_SIZE) | (i == len(title_ids) - 1):
                session_counter += counter
                counter = 0

                # update metadata file
                DataFrame(movie_metadata).to_json(
                    self._metadata_file,
                    storage_options=self._storage_options
                )

                self._logger.info(
                    f'Updated metadata file with {BATCH_SIZE} titles'
                )

                # stop program if we scraped many pages. This could be useful
                # if we have a limit on total running time (e.g. using
                # AWS Lambda)
                if session_counter >= self._chunk_size:
                    self._logger.info('Stop parsing due to requests limit')
                    return


def collect_original_title(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'hero-title-block__title'}
    try:
        return soup.find('h1', filters).text
    except Exception:
        return None


def collect_poster_url(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'hero-media__poster'}
    try:
        return soup.find('div', filters).img['src']
    except Exception:
        return None


def collect_review_summary(soup: BeautifulSoup)\
        -> Optional[Dict[str, Any]]:
    keys = ['user_review_num', 'critic_review_num', 'metascore']
    try:
        scores = [sc.text for sc in soup.find_all('span', class_=['score'])]
    except Exception:
        scores = [None, None, None]
    return dict(zip(keys, scores))


def collect_aggregate_rating(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    filters = {'data-testid': 'hero-rating-bar__aggregate-rating'}
    try:
        rating_raw = soup.find('div', filters).text
        rating, votes = (
            rating_raw
            .replace('IMDb RATING', '')
            .replace('/10', '/10?')
            .split('?')
        )
        return {'avg_rating': rating, 'num_votes': votes}
    except Exception:
        return None


def get_id_and_rank(s: str) -> Dict[str, Any]:
    id_ = s.split('?')[0] if s else None
    rank = s.split('_t_')[1] if s else None
    return id_, rank


def collect_actors(soup: BeautifulSoup) -> Dict[str, str]:
    filters = {'data-testid': 'title-cast-item__actor'}
    try:
        actors_raw = soup.find_all('a', filters)
        actors = {}
        for actor in actors_raw[:TOP_N_ACTORS]:
            id_, rank = get_id_and_rank(actor.get('href', None))
            actors[rank] = id_
        return actors
    except Exception:
        return {}


def collect_imdb_recommendations(soup: BeautifulSoup)\
        -> Optional[List[str]]:
    filters = {'class': re.compile('ipc-poster-card__title')}
    try:
        recom_raw = soup.find_all('a', filters)
        recommendations = {}
        for recom in recom_raw:
            id_, rank = get_id_and_rank(recom.get('href', None))
            recommendations[rank] = id_
        return recommendations
    except Exception:
        return {}


def collect_genres(soup: BeautifulSoup) -> Optional[List[str]]:
    filters = {'data-testid': 'genres'}
    try:
        genres_raw = soup.find('div', filters).find_all('a')
        return [el.text for el in genres_raw]
    except Exception:
        return None


def collect_details_summary(soup: BeautifulSoup)\
        -> Dict[str, Union[List[str], str]]:
    filters = {
        'release_date':
            {'data-testid': 'title-details-releasedate'},
        'countries_of_origin':
            {'data-testid': 'title-details-origin'},
        'language':
            {'data-testid': 'title-details-languages'},
        'also_known_as':
            {'data-testid': 'title-details-akas'},
        'production_companies':
            {'data-testid': 'title-details-companies'},
        'filming_locations':
            {'data-testid': 'title-details-filminglocations'}
    }
    details = {}
    for name, f in filters.items():
        try:
            raw_entity = soup.find('li', f).find_all('li')
            entity = [entry.text for entry in raw_entity]
        except Exception:
            entity = None
        details[name] = entity

    # add runtime info
    runtime_filter = {'data-testid': 'title-techspec_runtime'}
    try:
        runtime = soup.find('li', runtime_filter).div.text
    except Exception:
        runtime = None
    details['runtime'] = runtime

    return details


def collect_boxoffice(soup: BeautifulSoup) -> Optional[Dict[str, List[str]]]:
    filters = {
        'budget':
            {'data-testid': 'title-boxoffice-budget'},
        'boxoffice_gross_domestic':
            {'data-testid': 'title-boxoffice-grossdomestic'},
        'boxoffice_gross_opening':
            {'data-testid': 'title-boxoffice-openingweekenddomestic'},
        'boxoffice_gross_worldwide':
            {'data-testid': 'title-boxoffice-cumulativeworldwidegross'}
    }
    boxoffice = dict()
    for name, f in filters.items():
        try:
            entity = soup.find('li', f).li.text
        except Exception:
            entity = None
        boxoffice[name] = entity
    return boxoffice
