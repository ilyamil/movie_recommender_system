import os
import warnings
import requests
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from bs4 import BeautifulSoup
from bs4.element import Tag
from recsys.imdb_parser.identifiers import IDCollector
from recsys.utils import (wait, send_request,
                          create_logger, load_obj,
                          get_full_path, write_csv)

warnings.filterwarnings('ignore')


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
# START_URL_TEMPLATE = 'https://www.imdb.com{}reviews?ref_=tt_urv'
# LINK_URL_TEMPLATE = 'https://www.imdb.com{}reviews/_ajax'
START_URL_TEMPLATE = (
    'https://www.imdb.com{}reviews?sort=helpfulnessScore'
    '&dir=desc&ratingFilter=0'
)
LINK_URL_TEMPLATE = (
    'https://www.imdb.com{}reviews/_ajax/?sort=helpfulnessScore'
    '&dir=desc&ratingFilter=0'
)
BAR_FORMAT = '{desc:<20} {percentage:3.0f}%|{bar:20}{r_bar}'


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

    @staticmethod
    def collect_text(tag: Tag) -> Optional[str]:
        try:
            text_raw = tag.find('div', {'class': 'text show-more__control'})
            return text_raw.text
        except Exception:
            return None

    @staticmethod
    def collect_rating(tag: Tag) -> Optional[int]:
        try:
            rating_raw = tag.find_all('span')
            rating = rating_raw[1].text
            # If no rating was given, span block containes review date
            if len(rating) > 2:
                return None
            return int(rating)
        except Exception:
            return None

    @staticmethod
    def collect_date(tag: Tag) -> Optional[str]:
        try:
            date_raw = tag.find('span', {'class': 'review-date'})
            return date_raw.text
        except Exception:
            return None

    @staticmethod
    def collect_title(tag: Tag) -> Optional[str]:
        try:
            title_raw = tag.find('a', {'class': 'title'})
            return title_raw.text
        except Exception:
            return None

    @staticmethod
    def collect_author(tag: Tag) -> Optional[str]:
        try:
            author_raw = tag.find('span', {'class': 'display-name-link'})
            return author_raw.a['href']
        except Exception:
            return None

    @staticmethod
    def collect_helpfulness(tag: Tag) -> Optional[str]:
        try:
            helpfulness_raw = tag.find('div', {'class': 'actions text-muted'})
            return helpfulness_raw.text
        except Exception:
            return None

    @staticmethod
    def collect_review(id_: str, tag: Tag) -> Dict[str, Any]:
        return {
            'id': id_,
            'text': ReviewCollector.collect_text(tag),
            'rating': ReviewCollector.collect_rating(tag),
            'date': ReviewCollector.collect_date(tag),
            'title': ReviewCollector.collect_title(tag),
            'author': ReviewCollector.collect_author(tag),
            'helpfulness': ReviewCollector.collect_helpfulness(tag)
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
        test_id = '/title/tt0118767/'
        reviews = self.collect_title_reviews(test_id)
        print(len(reviews))
        # print('Collecting reviews...')
        # for genre in self._genres[:2]:
        #     genre_reviews = []
        #     genre_id_path = get_full_path(self._id_dir, f'{genre}.pkl')
        #     genre_id = load_obj(genre_id_path)
        #     for title_id in tqdm(genre_id, desc=genre, bar_format=BAR_FORMAT):
        #         title_reviews = self.collect_title_reviews(title_id)
        #         genre_reviews.extend(title_reviews)

        #     save_path = get_full_path(self._save_dir, f'{genre}.csv')
        #     write_csv(genre_reviews, save_path)

        #     self._logger.info(
        #         f'Total collected {len(genre_reviews)} reviews'
        #         f' in genre {genre.upper()}'
        #     )

        # test_id = '/title/tt0118767/'
        # test_reviews = self.collect_title_reviews(test_id)
        # filepath = get_full_path(self._save_dir, 'test.pkl')
        # dump_obj(test_reviews, filepath)

# def extract_text(tag: BeautifulSoup):
#     text_raw = tag.find('div', {'class': 'text show-more__control'})
#     return text_raw.text


# def extract_rating(tag: BeautifulSoup):
#     rating_raw = tag.find_all('span')
#     rating = rating_raw[1].text
#     # If no rating was given, span block containes review date
#     if len(rating) > 2:
#         return None
#     return float(rating)


# def extract_date(tag: BeautifulSoup) -> str:
#     date_raw = tag.find('span', {'class': 'review-date'})
#     return date_raw.text


# def extract_title(tag: BeautifulSoup) -> str:
#     title_raw = tag.find('a', {'class': 'title'})
#     return title_raw.text


# def extract_author(tag: BeautifulSoup) -> str:
#     author_raw = tag.find('span', {'class': 'display-name-link'})
#     return author_raw.a['href']


# def extract_helpfulness(tag: BeautifulSoup) -> str:
#     helpfulness_raw = tag.find('div', {'class': 'actions text-muted'})
#     return helpfulness_raw.text


# def extract_review(tag: BeautifulSoup) -> List[str]:
#     """Parses html tag to extract main a review's attributes.

#     Args:
#         tag (BeautifulSoup): html contaienr wrapped up by BS class.

#     Returns:
#         List[str]: Collection of attributes: text, rating,
#         date, title, author and helpfullnes of a review.
#     """
#     text = extract_text(tag)
#     rating = extract_rating(tag)
#     date = extract_date(tag)
#     title = extract_title(tag)
#     author = extract_author(tag)
#     helpfulness = extract_helpfulness(tag)
#     return [text, rating, date, title, author, helpfulness]


# def get_single_title_reviews(title_id: str,
#                              lowest_rating: int,
#                              highest_rating: int) -> List[List[str]]:
#     """Given title's id collects atrributes of the most helpfull reviews
#     having rating in certain borders.

#     Args:
#         title_id (str): title's id
#         lowest_rating (int): collects reviews with rating no less than this
#         value.
#         highest_rating (int): collects reviews with rating no greater than this
#         value.

#     Returns:
#         List[List[str]]: Collection of reviews' attributes, such as
#         review text, rating, date, author and helpfulness
#     """
#     movie_reviews = []
#     for rating in range(lowest_rating, highest_rating + 1):
#         url = REVIEWS_URL_TEMPLATE.format(title_id, rating)
#         soup = get_soup(url)
#         tags = soup.find_all('div', class_='review-tag')
#         if not tags:
#             continue

#         try:
#             reviews = [[title_id] + extract_review(cnt) for cnt in tags]
#             movie_reviews.extend(reviews)
#         except Exception as e:
#             print(f'Exception raised on title {title_id}, rating {rating}',
#                   f'with message {e}')

#         time.sleep(RATING_DELAY)

#     return movie_reviews


# def get_reviews(titles_id: List[str],
#                 lowest_rating: int = 1,
#                 highest_rating: int = 10) -> pd.DataFrame:
#     """Collects attributes of reviews of all passed identifiers.
#     Note: it's possible that not every rating be presented in a final dataset.

#     Args:
#         titles_id (List[str]): [description]
#         lowest_rating (int, optional): lower bound of review's rating.
#         Defaults to 1.
#         highest_rating (int, optional): upper bound of review's rating.
#         Defaults to 10.

#     Returns:
#         pandas.DataFrame: collected reviews' raw attributes for each title.
#     """
#     reviews = []
#     for title_id in tqdm(titles_id):
#         title_reviews = get_single_title_reviews(title_id, lowest_rating,
#                                                  highest_rating)
#         reviews.extend(title_reviews)
#         time.sleep(random.randint(MIN_DELAY, MAX_DELAY))

#     reviews_df = pandas.DataFrame(reviews, columns=COLUMNS)
#     return reviews_df
