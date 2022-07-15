import os
import warnings
import requests
from time import sleep
from typing import List, Dict, Any, Optional
from pandas import DataFrame, read_json
from tqdm import tqdm
from bs4 import BeautifulSoup
from bs4.element import Tag
from recsys.utils import send_request, create_logger

warnings.filterwarnings('ignore')


BAR_FORMAT = '{percentage:3.0f}%|{bar:20}{r_bar}'
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


import psutil


class ReviewCollector:
    def __init__(self, config: Dict[str, Any], credentials: Dict[str, Any]):
        self._bucket = config['bucket']
        self._review_folder = config['review_folder']
        self._n_reviews = config['n_reviews']
        self._chunk_size = config['chunk_size']
        self._pct_reviews = config['pct_reviews']
        self._sleep_time = config['sleep_time']

        self._logger = create_logger(
            filename=config['log_file'],
            msg_format=config['log_msg_format'],
            dt_format=config['log_dt_format'],
            level=config['log_level']
        )

        self._storage_options = {
            'key': credentials['access_key'],
            'secret': credentials['secret_access_key']
        }

        self._metadata_file = os.path.join(
            's3://', self._bucket, config['metadata_file']
        )
        self._metadata = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        if 'reviews_collected_flg' not in self._metadata.columns:
            self._metadata['reviews_collected_flg'] = 0
            self._metadata.to_json(
                self._metadata_file,
                storage_options=self._storage_options,
                orient='index'
            )
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
                sleep(self._sleep_time)

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

                if load_another_reviews:
                    link_url = LINK_URL_TEMPLATE.format(id_)
                    request_params['params']['paginationKey'] = pagination_key
                    try:
                        res = send_request(link_url, **request_params)
                    except Exception as e:
                        self._logger.warn(
                            f'Exception of sending link requests to ID {id_}'
                            f' with message: {e}'
                        )

                memusg = psutil.Process().memory_info().rss / (1024 * 1024)
                print(f'Collecting reviews for title {id_}. Current memory_usage {memusg:.2f} mb.')
        self._logger.info(
            f'Total collected {len(title_reviews)} reviews for title ID {id_}'
        )
        return title_reviews

    def is_all_reviews_collected(self) -> bool:
        """
        Checks if reviews were collected for all available titles.
        """
        # As status file has only one column it must be read in columnar
        # orientation
        metadata = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        already_collected = metadata['reviews_collected_flg'].sum()
        total_movies = len(metadata)

        print(
            f'Movie reviews are already collected for {already_collected}'
            + f' out of {total_movies} titles'
        )
        return total_movies == already_collected

    def collect(self) -> bool:
        print('Collecting reviews...')

        self._movie_metadata_df = read_json(
            self._metadata_file,
            storage_options=self._storage_options,
            orient='index'
        )
        movie_metadata = self._movie_metadata_df.T.to_dict()

        title_ids = [t for t, _ in movie_metadata.items()]
        counter = 0
        for title_id in tqdm(title_ids, bar_format=BAR_FORMAT, disable=True):
            if movie_metadata[title_id]['reviews_collected_flg']:
                continue

            memusg = psutil.Process().memory_info().rss / (1024 * 1024)
            print(f'Starting parsing {title_id}. Current memory_usage {memusg:.2f} mb.')

            title_reviews = self.collect_title_reviews(title_id)

            # This line extracts pure title id from raw form
            # e.g. '/title/tt0468569/' -> 'tt0468569'
            title_id_ = title_id.split('/')[-2]
            title_path = os.path.join(
                's3://',
                self._bucket,
                self._review_folder,
                title_id_ + '.csv'
            )
            try:
                if len(title_reviews) > 0:
                    DataFrame.from_records(title_reviews).to_csv(
                        title_path,
                        storage_options=self._storage_options
                    )

                # Update status on each iteration, because it takes a long
                # time to parse reviews even for a single title.
                movie_metadata[title_id]['reviews_collected_flg'] = 1
                DataFrame(movie_metadata).to_json(
                    self._metadata_file,
                    storage_options=self._storage_options
                )

                self._movie_metadata_df = read_json(
                    self._metadata_file,
                    storage_options=self._storage_options,
                    orient='index'
                )
                movie_metadata = self._movie_metadata_df.T.to_dict()

                counter += 1

                self._logger.info(
                    f'Collected {len(title_reviews)} reviews'
                    + f' about title {title_id_}'
                )
            except Exception as e:
                self._logger.warn(
                    f'Exception {str(e)} while collecting reviews'
                    + f' about title {title_id_}'
                )

            if counter == self._chunk_size:
                self._logger.info('Stop parsing due to requests limit')
                return True

        return True


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
