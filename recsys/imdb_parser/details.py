import re
from typing import Optional, Dict, List, Any
from bs4 import BeautifulSoup
from recsys.utils import send_request


BASE_URL = 'https://www.imdb.com{}'
TOP_N_ACTORS = 10


class DetailsCollector:
    def __init__(self) -> None:
        pass

    @staticmethod
    def collect_original_title(soup: BeautifulSoup) -> Optional[str]:
        filters = {'data-testid': 'hero-title-block__original-title'}
        try:
            return soup.find('div', filters).text
        except Exception:
            return None

    @staticmethod
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

    @staticmethod
    def collect_review_content(soup: BeautifulSoup)\
            -> Optional[Dict[str, Any]]:
        filters_rc = {'data-testid': 'reviewContent-all-reviews'}
        filters_el = {'class', 'three-Elements'}
        try:
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
        filters = {'class': re.compile('AggregateRatingButton')}
        try:
            return soup.find('div', filters).text
        except Exception:
            return None

    @staticmethod
    def collect_actors(soup: BeautifulSoup) -> Optional[Dict[str, str]]:
        filters = {'data-testid': 'title-cast-item__actor'}
        try:
            actors_raw = soup.find_all('a', filters)
            return {actor.text: actor.get('href', None)
                    for actor in actors_raw[:TOP_N_ACTORS]}
        except Exception:
            return None

    @staticmethod
    def collect_imdb_recommendations(soup: BeautifulSoup)\
            -> Optional[List[str]]:
        filters = {'class': re.compile('ipc-poster-card__title')}
        try:
            recom_raw = soup.find_all('a', filters)
            return [recom.get('href', None) for recom in recom_raw]
        except Exception:
            return None

    @staticmethod
    def collect_storyline(soup: BeautifulSoup) -> Optional[str]:
        filters = 'ipc-html-content ipc-html-content--base'
        try:
            return soup.find('div', {'class': filters}).text
        except Exception:
            return None

    @staticmethod
    def collect_tagline(soup: BeautifulSoup) -> Optional[str]:
        filters = {'data-testid': 'storyline-taglines'}
        try:
            return soup.find('li', filters).text
        except Exception:
            return None

    @staticmethod
    def collect_genres(soup: BeautifulSoup) -> Optional[str]:
        filters = {'data-testid': 'storyline-genres'}
        try:
            return soup.find('li', filters).text
        except Exception:
            return None

    @staticmethod
    def collect_certificate(soup: BeautifulSoup) -> Optional[str]:
        filters = {'data-testid': 'storyline-certificate'}
        try:
            return soup.find('li', filters).text
        except Exception:
            return None

    @staticmethod
    def collect_details(soup: BeautifulSoup) -> Dict[str, str]:
        details_sections = [
            'Release date',
            'Country of origin',
            'Official site',
            'Languages',
            'Also known as',
            'Filming locations',
            'Production companies'
        ]
        filters = {'data-testid': 'title-details-section'}
        try:
            details = soup.find('div', filters).text
            return extract_substrings_after_anchors(details, details_sections)
        except Exception:
            return dict.fromkeys(details_sections)

    @staticmethod
    def collect_title_details(soup: BeautifulSoup) -> Dict[str, Any]:
        return {
            'original_title': DetailsCollector.collect_original_title(soup),
            'poster': DetailsCollector.collect_poster(soup),
            'review_content': DetailsCollector.collect_review_content(soup),
            'agg_rating': DetailsCollector.collect_aggregate_rating(soup),
            'actors': DetailsCollector.collect_actors(soup)
        }

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


def extract_substrings_after_anchors(s: str, anchors: List[str])\
        -> Optional[Dict[str, str]]:
    details = {}
    empty_anchors = []
    use_anchors = []
    for anchor in anchors:
        if anchor not in s:
            empty_anchors.append(anchor)
        else:
            use_anchors.append(anchor)
    for section_num in range(len(use_anchors)):
        start = use_anchors[section_num]
        left_loc = s.find(start)
        if section_num != len(use_anchors) - 1:
            end = use_anchors[section_num + 1]
            right_loc = s.rfind(end)
            details[start] = s[left_loc + len(start): right_loc]
        else:
            details[start] = s[left_loc + len(start):]
    details.update(**dict.fromkeys(empty_anchors))
    return details
