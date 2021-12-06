import re
from typing import Optional, Dict, Any
from bs4 import BeautifulSoup
from recsys.utils import send_request


BASE_URL = 'https://www.imdb.com{}'


class DetailsCollector:
    def __init__(self) -> None:
        pass

    @staticmethod
    def collect_original_title(soup: BeautifulSoup) -> Optional[str]:
        try:
            filters = {
                'data-testid': 'hero-title-block__original-title'
            }
            return (
                soup
                .find('div', filters)
                .text
            )
        except Exception:
            return None

    @staticmethod
    def collect_poster(soup: BeautifulSoup) -> Optional[bytes]:
        try:
            filters = {'aria-label': 'View {Title} Poster'}
            img_id = (
                soup
                .find('a', filters)
                .get('href')
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

    @staticmethod
    def collect_review_content(soup: BeautifulSoup)\
            -> Optional[Dict[str, Any]]:
        try:
            filters_rc = {'data-testid': 'reviewContent-all-reviews'}
            filters_el = {'class', 'three-Elements'}
            review_content_raw = (
                soup
                .find('ul', filters_rc)
                .find_all('span', filters_el)
            )
        except Exception:
            return {
                'n_user_reviews': None,
                'n_critic_reviews': None,
                'metascore': None,
            }

        try:
            user_reviews = review_content_raw[0].text
        except Exception:
            user_reviews = None

        try:
            critic_reviews = review_content_raw[1].text
        except Exception:
            critic_reviews = None

        try:
            metascore = review_content_raw[2].text
        except Exception:
            metascore = None

        return {
            'n_user_reviews': user_reviews,
            'n_critic_reviews': critic_reviews,
            'metascore': metascore,
        }

    @staticmethod
    def collect_aggregate_rating(soup: BeautifulSoup) -> Optional[str]:
        try:
            regexp = re.compile('AggregateRatingButton')
            return (
                soup
                .find('div', {'class', regexp})
                .text
            )
        except Exception:
            return None

    @staticmethod
    def collect_title_details(soup: BeautifulSoup) -> Dict[str, Any]:
        return {
            'original_title': DetailsCollector.collect_original_title(soup),
            'poster': DetailsCollector.collect_poster(soup),
            'review_content': DetailsCollector.collect_review_content(soup),
            'agg_rating': DetailsCollector.collect_aggregate_rating(soup),
            'a': 1
        }
