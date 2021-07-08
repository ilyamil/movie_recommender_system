import re
from recsys.parser.src.review_extraction import MAX_DELAY, MIN_DELAY
import time
import random
import requests
from typing import Union, List, Optional
from bs4 import BeautifulSoup
from tqdm import tqdm
from src.utils import get_soup, dump_obj, load_obj


URL_TEMPLATE = 'https://www.imdb.com/search/title/?genres={}'\
                '&sort=num_votes,desc&start={}&explore=genres'
STEP = 50
MIN_DELAY = 1
MAX_DELAY = 2


def get_filename(genre: str, rank: int, version: int) -> str:
    return f'{genre}_from_{str(rank)}_to_{str(rank + STEP)}_v{version}'


class MovieIDExtractor:
    """Contains methods to load and parse web pages,
    then extract movie IDs.


    """
    def __init__(self,
                 genres: Union[List[str], str],
                 n_titles: int,
                 min_delay: int,
                 max_delay: int):
        if not isinstance(genres, list):
            self.genres = [genres]
        self.n_titles = n_titles
        self.min_delay = min_delay
        self.max_delay = max_delay

    @staticmethod
    def get_max_titles(page: BeautifulSoup) -> int:
        tag_text = page.find('div', class_='desc')
        max_titles = re.search('of(.+?)title', tag_text.span.text)
        return int(max_titles.group(1).strip().replace(',', ''))

    @staticmethod
    def get_page_titles(container: BeautifulSoup) -> List[str]:
        titles_raw = container.find_all('h3', class_='lister-item-header')
        return [title.a['href'] for title in titles_raw]

    def get_html_DOM(self, path: str, version: int) -> None:
        """[summary]

        Args:
            path (str): [description]
            version (int): [description]

        Returns:
            [type]: [description]
        """
        msg_success = 'Dumped html page with rank {} - {} in {} genre'
        msg_failure = 'Bad status code for rank {} - {} in {} genre'
        msg_exception = 'Exception with message {}'

        for genre in tqdm(self.genres):
            url = URL_TEMPLATE.format(genre, 1)
            soup = get_soup(url)
            max_titles = self.get_max_titles(soup)

            time.sleep(random.randint(self.min_delay, self.max_delay))
            for rank in range(1, min(self.n_titles, max_titles), STEP):
                try:
                    url = URL_TEMPLATE.format(genre, rank)
                    page = requests.get(url)
                    if page.status_code == 200:
                        filename = get_filename(genre, rank)
                        dump_obj(page.content, write_dir + filename)
                        msg 
                except Exception as e:
                    
                time.sleep(random.randint(min_delay, max_delay))

            msg = f'Collected {n_collected} titles in {genre.upper()} genre'
            print(msg)

        titles = list(set(titles))
        print(f'Total collected {len(titles)} unique titles')

        return titles

    def parse_html(self, path: str, version: int) -> None:
        pass



def get_popular_titles(genres: Union[List[str], str], n_titles: int)\
     -> List[str]:
    """Scrape title's identifiers in specified genres from IMDB web site.

    Args:
        genres (Union[list, str]): names of genres.
        n_titles (int): maximum number of identifiers in each genre
        we want to collect.

    Returns:
        List[str]: collection of unique identifiers.
    """
    if isinstance(genres, str) and genres == 'all':
        genres = ALL_GENRES
    if not isinstance(genres, list):
        genres = [genres]

    titles = []
    for genre in tqdm(genres):
        old_length = len(titles)

        url = GENRE_URL_TEMPLATE.format(genre, 1)
        soup = get_soup(url)
        max_titles = get_max_titles(soup)

        time.sleep(random.randint(MIN_DELAY, MAX_DELAY))
        for start_rank in range(1, min(n_titles, max_titles), STEP):
            url = GENRE_URL_TEMPLATE.format(genre, start_rank)
            soup = get_soup(url)
            page_titles = get_page_titles(soup)
            titles.extend(page_titles)

            time.sleep(random.randint(MIN_DELAY, MAX_DELAY))

        n_collected = len(titles) - old_length
        msg = f'Collected {n_collected} titles in {genre.upper()} genre'
        print(msg)

    titles = list(set(titles))
    print(f'Total collected {len(titles)} unique titles')

    return titles
