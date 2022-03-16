import os
import re
from typing import Optional, Dict, List, Any
from bs4 import BeautifulSoup
from tqdm import tqdm
from recsys.utils import (send_request, create_logger,
                          write_csv, load_obj, get_full_path,
                          write_bytest_to_image, wait)


BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'
BASE_URL = 'https://www.imdb.com{}'
TOP_N_ACTORS = 10


class DetailsCollector:
    def __init__(self, collector_config: Dict[str, Any],
                 logger_config: Dict[str, Any]) -> None:
        log_file = collector_config['log_file']
        self._logger = create_logger(logger_config, log_file)
        self._save_dir = collector_config['dir']
        self._id_dir = collector_config['id_dir']
        self._poster_dir = collector_config['poster_dir']
        self._genres = collector_config['genres']
        self._min_delay = collector_config['request_delay']['min_delay']
        self._max_delay = collector_config['request_delay']['max_delay']

        if not isinstance(self._genres, list):
            self._genres = [self._genres]

        id_path = get_full_path(self._id_dir)
        available_genres = [
            genre.split('.')[0]
            for genre in os.listdir(id_path)
        ]
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
        return {
            'original_title': collect_original_title(soup),
            'poster': collect_poster(soup),
            'review_summary': collect_review_summary(soup),
            'agg_rating': collect_aggregate_rating(soup),
            'actors': collect_actors(soup),
            'imdb_recommendations': collect_imdb_recommendations(soup),
            'storyline': collect_storyline(soup),
            'tagline': collect_tagline(soup),
            'certificate': collect_certificate(soup),
            'details': collect_details_summary(soup),
            'boxoffice': collect_boxoffice(soup),
            'techspecs': collect_techspecs(soup)
        }

    def collect(self) -> None:
        print('Collecting details...')
        for genre in self._genres:
            genre_details = []
            genre_path = get_full_path(self._id_dir, f'{genre}.pkl')
            tqdm_params = {
                'iterable': load_obj(genre_path),
                'desc': genre,
                'bar_format': BAR_FORMAT
            }
            for title_id in tqdm(**tqdm_params):
                title_url = BASE_URL.format(title_id)
                try:
                    response = send_request(title_url)
                    soup = BeautifulSoup(response.text, 'lxml')
                    details = {
                        'title_id': title_id,
                        **self.collect_title_details(soup)
                    }
                    details['title_id'] = title_id
                    # saving poster on disk
                    try:
                        poster = details['poster']
                        poster_name = f'{title_id.split("/")[-2]}.jpeg'
                        poster_path = get_full_path(
                            self._poster_dir, poster_name
                        )
                        write_bytest_to_image(poster, poster_path)
                    except Exception as e:
                        self._logger.warn(
                            f'Exception in saving poster for title {title_id}'
                            f' with message: {e}'
                        )

                    del details['poster']
                    genre_details.append(details)

                    wait(self._min_delay, self._max_delay)
                except Exception:
                    self._logger.warn(
                        f'Exception in parsing {title_url}'
                    )

            save_path = get_full_path(self._save_dir, f'{genre}.csv')
            write_csv(genre_details, save_path)

            self._logger.info(
                f'Total collected {len(genre_details)} reviews'
                f' in genre {genre.upper()}'
            )


def collect_original_title(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'hero-title-block__original-title'}
    try:
        return soup.find('div', filters).text
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
    keys = ['user_reviews_num', 'criti_review_num', 'metascore']
    try:
        scores = [sc.text for sc in soup.find_all('span', class_=['score'])]
    except Exception:
        scores = [None, None, None]
    return dict(zip(keys, scores))


def collect_aggregate_rating(soup: BeautifulSoup) -> Optional[str]:
    filters = {'class': re.compile('AggregateRatingButton')}
    try:
        return soup.find('div', filters).text
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


def collect_storyline(soup: BeautifulSoup) -> Optional[str]:
    filters = 'ipc-html-content ipc-html-content--base'
    try:
        return soup.find('div', {'class': filters}).text
    except Exception:
        return None


def collect_tagline(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'storyline-taglines'}
    try:
        return soup.find('li', filters).text
    except Exception:
        return None


def collect_genres(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'storyline-genres'}
    try:
        return soup.find('li', filters).text
    except Exception:
        return None


def collect_certificate(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'storyline-certificate'}
    try:
        return soup.find('li', filters).text
    except Exception:
        return None


def collect_details_summary(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'title-details-section'}
    try:
        return soup.find('div', filters).text
    except Exception:
        return None


def collect_boxoffice(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'title-boxoffice-section'}
    try:
        return soup.find('div', filters).text
    except Exception:
        return None


def collect_techspecs(soup: BeautifulSoup) -> Optional[str]:
    filters = {'data-testid': 'title-techspecs-section'}
    try:
        return soup.find('div', filters).text
    except Exception:
        return None
