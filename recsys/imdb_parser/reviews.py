import os
import warnings
import requests
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from bs4 import BeautifulSoup
from bs4.element import Tag
from recsys.utils import (wait, send_request,
                          create_logger, load_obj,
                          get_full_path, write_csv)

warnings.filterwarnings('ignore')


BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'
COLUMNS = [
    'title_id',
    'text',
    'rating',
    'date',
    'title',
    'author',
    'helpfulness'
]
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 6.1)'
    'AppleWebKit/537.36 (KHTML, like Gecko)'
    'Chrome/88.0.4324.150 Safari/537.36'
)
START_URL_TEMPLATE = (
    'https://www.imdb.com{}reviews?sort=helpfulnessScore'
    '&dir=desc&ratingFilter=0'
)
LINK_URL_TEMPLATE = (
    'https://www.imdb.com{}reviews/_ajax/?sort=helpfulnessScore'
    '&dir=desc&ratingFilter=0'
)


class ReviewCollector:
    def __init__(self, collector_config: Dict[str, Any],
                 logger_config: Dict[str, Any]) -> None:
        log_file = collector_config['log_file']
        self._logger = create_logger(logger_config, log_file)
        self._save_dir = collector_config['dir']
        self._id_dir = collector_config['id_dir']
        self._genres = collector_config['genres']
        self._n_reviews = collector_config['n_reviews']
        self._pct_reviews = collector_config['pct_reviews']
        self._min_delay = collector_config['request_delay']['min_delay']
        self._max_delay = collector_config['request_delay']['max_delay']

        if not isinstance(self._genres, list):
            self._genres = [self._genres]

        available_genres = [
            genre.split('.')[0]
            for genre in os.listdir(get_full_path(self._id_dir))
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

        if not (self._pct_reviews or self._n_reviews):
            raise ValueError(
                'Only one of these arguments needs to be set'
                ' in config file: n_reviews or pct_reviews'
            )

        if self._pct_reviews and not 0 <= self._pct_reviews <= 100:
            raise ValueError(
                    'pct_reviews must lie in the interval [0, 100]'
            )

        self._logger.info('Successfully initialized ReviewCollector')

    @staticmethod
    def collect_review(id_: str, tag: Tag) -> Dict[str, Any]:
        return {
            'id': id_,
            'text': collect_text(tag),
            'rating': collect_rating(tag),
            'date': collect_date(tag),
            'title': collect_title(tag),
            'author': collect_author(tag),
            'helpfulness': collect_helpfulness(tag)
        }

    @staticmethod
    def find_reviews_num(response: requests.Response) -> int:
        bs = BeautifulSoup(response.text, 'lxml')
        review_cnt = (
            bs
            .find('div', {'class', 'header'})
            .find('div')
            .text
            .replace(' ', '')
            .replace(',', '')
            .split('Reviews')[0]
        )
        return int(review_cnt)

    def collect_title_reviews(self, id_: str) -> List[Dict[str, Any]]:
        request_params = {
            'params': {
                'ref_': 'undefined',
                'paginationKey': ''
            }
        }

        with requests.Session() as session:
            session.headers['User-Agent'] = USER_AGENT
            start_url = START_URL_TEMPLATE.format(id_)
            try:
                res = send_request(start_url, session=session)
            except Exception as e:
                self._logger.warn(
                    f'Exception of sending start requests to ID {id_}'
                    f' with message: {e}'
                )

            reviews_num = ReviewCollector.find_reviews_num(res)
            if self._n_reviews:
                reviews_num_max = self._n_reviews
            elif self._pct_reviews:
                reviews_num_max = int(reviews_num * self._pct_reviews / 100)

            title_reviews = []
            load_another_reviews = True
            while load_another_reviews:
                wait(self._min_delay, self._max_delay)

                reviews_batch = []
                soup = BeautifulSoup(res.text, 'lxml')
                for tag in soup.select('.review-container'):
                    review = ReviewCollector.collect_review(id_, tag)
                    reviews_batch.append(review)

                title_reviews.extend(reviews_batch)
                self._logger.info(
                    f'Collected {len(reviews_batch)} reviews'
                    f' for title ID {id_}'
                )

                if len(title_reviews) > reviews_num_max:
                    break

                # imitate clicking load-more button
                try:
                    pagination_key = (
                            soup
                            .select_one(".load-more-data[data-key]")
                            .get("data-key")
                    )
                except AttributeError:
                    load_another_reviews = False

                link_url = LINK_URL_TEMPLATE.format(id_)
                request_params['params']['paginationKey'] = pagination_key
                try:
                    res = send_request(link_url, **request_params)
                except Exception as e:
                    self._logger.warn(
                        f'Exception of sending link requests to ID {id_}'
                        f' with message: {e}'
                    )

        self._logger.info(
            f'Total collected {len(title_reviews)} reviews for title ID {id_}'
        )
        return title_reviews

    def collect(self) -> None:
        print('Collecting reviews...')
        for genre in self._genres:
            genre_reviews = []
            genre_id_path = get_full_path(self._id_dir, f'{genre}.pkl')
            genre_id = load_obj(genre_id_path)
            for title_id in tqdm(genre_id, desc=genre, bar_format=BAR_FORMAT):
                title_reviews = self.collect_title_reviews(title_id)
                genre_reviews.extend(title_reviews)

            save_path = get_full_path(self._save_dir, f'{genre}.csv')
            write_csv(genre_reviews, save_path)

            self._logger.info(
                f'Total collected {len(genre_reviews)} reviews'
                f' in genre {genre.upper()}'
            )


def collect_date(tag: Tag) -> Optional[str]:
    filters = {'class': 'review-date'}
    try:
        date_raw = tag.find('span', filters)
        return date_raw.text
    except Exception:
        return None


def collect_title(tag: Tag) -> Optional[str]:
    filters = {'class': 'title'}
    try:
        title_raw = tag.find('a', filters)
        return title_raw.text
    except Exception:
        return None


def collect_text(tag: Tag) -> Optional[str]:
    filters = {'class': 'text show-more__control'}
    try:
        text_raw = tag.find('div', filters)
        return text_raw.text
    except Exception:
        return None


def collect_rating(tag: Tag) -> Optional[float]:
    try:
        rating_raw = tag.find_all('span')
        rating = rating_raw[1].text
        # If no rating was given, span block containes review date
        if len(rating) > 2:
            return None
        return int(rating)
    except Exception:
        return None


def collect_author(tag: Tag) -> Optional[str]:
    filters = {'class': 'display-name-link'}
    try:
        author_raw = tag.find('span', filters)
        return author_raw.a['href']
    except Exception:
        return None


def collect_helpfulness(tag: Tag) -> Optional[str]:
    filters = {'class': 'actions text-muted'}
    try:
        helpfulness_raw = tag.find('div', filters)
        return helpfulness_raw.text
    except Exception:
        return None
