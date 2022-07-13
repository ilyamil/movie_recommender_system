import re
from time import sleep
from typing import Optional, Dict, List, Any, Union
from pandas import DataFrame, read_json
from bs4 import BeautifulSoup
from tqdm import tqdm
from recsys.utils import send_request, create_logger


BAR_FORMAT = '{percentage:3.0f}%|{bar:20}{r_bar}'
BASE_URL = 'https://www.imdb.com{}'
TOP_N_ACTORS = 10
BATCH_SIZE = 50


class MetadataCollector:
    def __init__(self, config: Dict[str, Any], credentials: Dict[str, Any]):
        self._metadata_file = config['metadata_file']
        self._genres = config['genres']
        self._chunk_size = config['chunk_size']
        self._sleep_time = config['sleep_time']

        self._storage_options = {
            'key': credentials['access_key'],
            'secret': credentials['secret_access_key']
        }

        self._logger = create_logger(
            filename=config['log_file'],
            msg_format=config['log_msg_format'],
            dt_format=config['log_dt_format'],
            level=config['log_level']
        )

        if not isinstance(self._genres, list):
            self._genres = [self._genres]

        self._genres = [genre.lower() for genre in self._genres]

        movie_metadata = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        available_genres = {
            item['main_genre'].lower()
            for item in movie_metadata.T.to_dict().values()
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
        return already_collected == total_movies

    def collect(self) -> None:
        print('Collecting metadata...')

        movie_metadata_df = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        movie_metadata = movie_metadata_df.T.to_dict()

        title_ids = [t for t, _ in movie_metadata.items()]
        counter = 0
        total_counter = 0
        for title_id in tqdm(title_ids, bar_format=BAR_FORMAT):
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
            if counter == BATCH_SIZE:
                total_counter += counter
                counter = 0

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
                if total_counter >= self._chunk_size:
                    self._logger.info('Stop parsing due to limit')
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
