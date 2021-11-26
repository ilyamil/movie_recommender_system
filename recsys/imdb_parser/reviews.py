import warnings
import requests
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from bs4 import BeautifulSoup
from bs4.element import Tag
from recsys.utils import wait, send_request

warnings.filterwarnings('ignore')


# REVIEWS_URL_TEMPLATE = (
#     'https://www.imdb.com{}'
#     'reviews?sort=helpfulnessScore&dir=desc&ratingFilter={}'
# )
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
START_URL_TEMPLATE = 'https://www.imdb.com{}reviews?ref_=tt_urv'
LINK_URL_TEMPLATE = 'https://www.imdb.com{}reviews/_ajax'


class ReviewCollector:
    def __init__(self) -> None:
        pass

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

    def collect_review(self, id_: str, tag: Tag) -> Dict[str, Any]:
        return {
            'id': id_,
            'text': ReviewCollector.collect_text(tag),
            'rating': ReviewCollector.collect_rating(tag),
            'date': ReviewCollector.collect_date(tag),
            'title': ReviewCollector.collect_title(tag),
            'author': ReviewCollector.collect_author(tag),
            'helpfulness': ReviewCollector.collect_helpfulness(tag)
        }

    def collect_id_reviews(self, id_: str) -> List[Dict[str, Any]]:
        request_params = {
            'params': {
                'ref_': 'undefined',
                'paginationKey': ''
            }
        }

        with requests.Session() as session:
            session.headers['User-Agent'] = USER_AGENT
            start_url = START_URL_TEMPLATE.format(id_)
            res = send_request(start_url, session=session)

            id_reviews = []
            while True:
                soup = BeautifulSoup(res.text, 'lxml')
                for tag in soup.select('.review-tag'):
                    review = ReviewCollector.collect_review(id_, tag)
                    id_reviews.append(review)

                    try:
                        pagination_key = (
                            soup
                            .select_one(".load-more-data[data-key]")
                            .get("data-key")
                        )
                    except AttributeError:
                        break

                    link_url = LINK_URL_TEMPLATE.format(id_)
                    request_params['params']['paginationKey'] = pagination_key
                    res = send_request(link_url, **request_params)

    def collect(self) -> None:
        test_id = '/title/tt0238883/'

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
