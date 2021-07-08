import time
import random
import pandas
from typing import List

from bs4 import BeautifulSoup
from tqdm import tqdm

from src.utils import get_soup

import warnings
warnings.filterwarnings('ignore')


REVIEWS_URL_TEMPLATE = 'https://www.imdb.com{}'\
    + 'reviews?sort=helpfulnessScore&dir=desc&ratingFilter={}'
COLUMNS = [
    'movie_id',
    'text',
    'rating',
    'date',
    'title',
    'author',
    'helpfulness'
]
MIN_DELAY = 1
MAX_DELAY = 2
RATING_DELAY = 1


def extract_text(container: BeautifulSoup):
    text_raw = container.find('div', {'class': 'text show-more__control'})
    return text_raw.text


def extract_rating(container: BeautifulSoup):
    rating_raw = container.find_all('span')
    rating = rating_raw[1].text
    # If no rating was given, span block containes review date
    if len(rating) > 2:
        return None
    return float(rating)


def extract_date(container: BeautifulSoup) -> str:
    date_raw = container.find('span', {'class': 'review-date'})
    return date_raw.text


def extract_title(container: BeautifulSoup) -> str:
    title_raw = container.find('a', {'class': 'title'})
    return title_raw.text


def extract_author(container: BeautifulSoup) -> str:
    author_raw = container.find('span', {'class': 'display-name-link'})
    return author_raw.a['href']


def extract_helpfulness(container: BeautifulSoup) -> str:
    helpfulness_raw = container.find('div', {'class': 'actions text-muted'})
    return helpfulness_raw.text


def extract_review(container: BeautifulSoup) -> List[str]:
    """Parses html container to extract main a review's attributes.

    Args:
        container (BeautifulSoup): html contaienr wrapped up by BS class.

    Returns:
        List[str]: Collection of attributes: text, rating,
        date, title, author and helpfullnes of a review.
    """
    text = extract_text(container)
    rating = extract_rating(container)
    date = extract_date(container)
    title = extract_title(container)
    author = extract_author(container)
    helpfulness = extract_helpfulness(container)
    return [text, rating, date, title, author, helpfulness]


def get_single_title_reviews(title_id: str,
                             lowest_rating: int,
                             highest_rating: int) -> List[List[str]]:
    """Given title's id collects atrributes of the most helpfull reviews
    having rating in certain borders.

    Args:
        title_id (str): title's id
        lowest_rating (int): collects reviews with rating no less than this
        value.
        highest_rating (int): collects reviews with rating no greater than this
        value.

    Returns:
        List[List[str]]: Collection of reviews' attributes, such as
        review text, rating, date, author and helpfulness
    """
    movie_reviews = []
    for rating in range(lowest_rating, highest_rating + 1):
        url = REVIEWS_URL_TEMPLATE.format(title_id, rating)
        soup = get_soup(url)
        containers = soup.find_all('div', class_='review-container')
        if not containers:
            continue

        try:
            reviews = [[title_id] + extract_review(cnt) for cnt in containers]
            movie_reviews.extend(reviews)
        except Exception as e:
            print(f'Exception raised on title {title_id}, rating {rating}',
                  f'with message {e}')

        time.sleep(RATING_DELAY)

    return movie_reviews


def get_reviews(titles_id: List[str],
                lowest_rating: int = 1,
                highest_rating: int = 10) -> pandas.DataFrame:
    """Collects attributes of reviews of all passed identifiers.
    Note: it's possible that not every rating be presented in a final dataset.

    Args:
        titles_id (List[str]): [description]
        lowest_rating (int, optional): lower bound of review's rating.
        Defaults to 1.
        highest_rating (int, optional): upper bound of review's rating.
        Defaults to 10.

    Returns:
        pandas.DataFrame: collected reviews' raw attributes for each title.
    """
    reviews = []
    for title_id in tqdm(titles_id):
        title_reviews = get_single_title_reviews(title_id, lowest_rating,
                                                 highest_rating)
        reviews.extend(title_reviews)
        time.sleep(random.randint(MIN_DELAY, MAX_DELAY))

    reviews_df = pandas.DataFrame(reviews, columns=COLUMNS)
    return reviews_df
