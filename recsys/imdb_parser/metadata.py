import re
from typing import Optional, Dict, List, Any, Union
from bs4 import BeautifulSoup
from tqdm import tqdm
from recsys.utils import (save_img, send_request, create_logger,
                          read_json, write_json, get_full_path,
                          wait)
from boto3 import client


BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'
BASE_URL = 'https://www.imdb.com{}'
TOP_N_ACTORS = 10


class MetadataCollector:
    def __init__(self, config: Dict[str, Any],
                 credentials: Dict[str, str] = None):
        self._config = config
        self._metadata_file = config['metadata_file']
        self._genres = config['genres']
        self._min_delay = config['min_delay']
        self._max_delay = config['max_delay']

        if config['s3_poster_dir']:
            if not credentials:
                raise ValueError(
                    'No credentials were passed.'
                    + ' You must either pass credentials'
                    + ' or set a field "s3_poster_dir" to null in config file'
                )
            self._s3_client = client(
                's3',
                aws_access_key_id=credentials['access_key'],
                aws_secret_access_key=credentials['secret_access_key']
            )
            self._poster_dir = config['s3_poster_dir']
        else:
            self._poster_dir = config['poster_dir']

        self._logger = create_logger(filename=config['log_file'],
                                     msg_format=config['log_msg_format'],
                                     dt_format=config['log_dt_format'],
                                     level=config['log_level'])

        if not isinstance(self._genres, list):
            self._genres = [self._genres]

        self._movie_metadata = read_json(get_full_path(self._metadata_file))
        available_genres = {
            item['main_genre']
            for item in self._movie_metadata.values()
        }
        if 'all' not in self._genres:
            use_genres = set(self._genres).intersection(available_genres)
            genre_diff = set(self._genres) - set(use_genres)
            if genre_diff:
                self._logger.warning(
                    f'No {", ".join(genre_diff)} in possible genres'
                )
            if not use_genres:
                raise ValueError('No valid genres were passed')
            self._genres = use_genres
        else:
            self._genres = available_genres

        self._logger.info('Successfully initialized DetailsCollector')

    @staticmethod
    def collect_title_details(soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Collects the following details (if exists) about a single movie:
            * original title
            * poster
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
            'poster': collect_poster(soup),
            'review_summary': collect_review_summary(soup),
            'agg_rating': collect_aggregate_rating(soup),
            'actors': collect_actors(soup),
            'imdb_recommendations': collect_imdb_recommendations(soup),
            'details': collect_details_summary(soup),
            'boxoffice': collect_boxoffice(soup)
        }

    def collect(self) -> None:
        print('Collecting details...')
        for genre in self._genres:
            title_ids = [
                t for t, v in self._movie_metadata.items()
                if v['main_genre'] == genre
            ]
            for title_id in tqdm(title_ids, genre, bar_format=BAR_FORMAT):
                title_url = BASE_URL.format(title_id)
                try:
                    response = send_request(title_url)
                    soup = BeautifulSoup(response.text, 'lxml')
                    details = self.collect_title_details(soup)
                    # saving poster on disk
                    try:
                        id_ = title_id.replace('/title/', '')[:-1]
                        save_img(
                            img=details['poster'],
                            img_name=f'{id_}.jpeg',
                            config=self._config,
                            s3_client=self._s3_client
                        )
                        self._logger.info(
                            f'Collected metadata for title {title_id}'
                        )
                    except Exception as e:
                        self._logger.warn(
                            f'Exception in saving poster for title {title_id}'
                            f' with message: {e}'
                        )
                    del details['poster']
                    self._movie_metadata[title_id] |= details
                except Exception:
                    self._logger.warn(
                        f'Exception in parsing {title_url}'
                    )

                wait(self._min_delay, self._max_delay)

            filepath = get_full_path(self._metadata_file)
            write_json(self._movie_metadata, filepath)

            self._logger.info(
                f'Collected metadata for all titles in genre {genre}'
            )


def collect_original_title(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'hero-title-block__title'}
    try:
        return soup.find('h1', filters).text
    except Exception:
        return None


def collect_poster(soup: BeautifulSoup) -> Optional[bytes]:
    filters = {'aria-label': 'View {Title} Poster'}
    try:
        img_id = (
            soup
            .find('a', filters)
            .get('href', None)
            .split('?')[0]
        )
        inter_url = BASE_URL.format(img_id)
        inter_response = send_request(inter_url)
        img_download_link = (
            BeautifulSoup(inter_response.text, 'lxml')
            .find('img')['src']
        )
        response = send_request(img_download_link)
        return response.content
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


def collect_actors(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
    filters = {'data-testid': 'title-cast-item__actor'}
    try:
        actors_raw = soup.find_all('a', filters)
        return {actor.text: actor.get('href', None)
                for actor in actors_raw[:TOP_N_ACTORS]}
    except Exception:
        return None


def collect_imdb_recommendations(soup: BeautifulSoup)\
        -> Optional[List[str]]:
    filters = {'class': re.compile('ipc-poster-card__title')}
    try:
        recom_raw = soup.find_all('a', filters)
        return [recom.get('href', None) for recom in recom_raw]
    except Exception:
        return None


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
