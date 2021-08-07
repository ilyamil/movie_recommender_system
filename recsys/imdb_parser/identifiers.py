import re
import requests
from tqdm import tqdm
from typing import Union, List
from bs4 import BeautifulSoup
from recsys.utils import dump_obj, get_filepath, wait


URL_TEMPLATE = (
    'https://www.imdb.com/search/title/?genres={}'
    + '&sort=num_votes,desc&start={}&explore=genres'
)
STEP = 50


class IDCollector:
    """Contains methods to load and parse web pages,
    then extract movie IDs.

    Public methods:

    """
    def __init__(self, genres: Union[List[str], str], n_titles: int,
                 min_delay: int = 1, max_delay: int = 2):
        self.genres = genres if isinstance(genres, list) else [genres]
        self.n_titles = n_titles
        self.min_delay = min_delay
        self.max_delay = max_delay

    def _get_num_of_movies(page_content: bytes) -> int:
        page_html = BeautifulSoup(page_content, 'html.parser')
        tag_text = page_html.find('div', class_='desc')
        max_titles = re.search('of(.+?)title', tag_text.span.text)
        return int(max_titles.group(1).strip().replace(',', ''))

    def _get_movie_id(page_content: bytes) -> List[str]:
        page_html = BeautifulSoup(page_content, 'html.parser')
        titles_raw = page_html.find_all('h3', class_='lister-item-header')
        return [title.a['href'] for title in titles_raw]

    def _collect_genre_id(self, genre: str, max_titles: int) -> List[int]:
        """[summary]

        Args:
            genre (str): [description]
            max_titles (int): [description]

        Returns:
            List[int]: [description]
        """
        genre_id = []
        for rank in range(1, min(self.n_titles, max_titles), STEP):
            url = URL_TEMPLATE.format(genre, rank)
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    rank_id = self._get_movie_id(response.content)
                    genre_id += rank_id
                    msg = (
                        f'Collected {len(rank_id)} IDs'
                        + f'in genre {genre.upper()},'
                        + f'rank {rank}-{rank + STEP}'
                    )
                    print(msg)
                else:
                    msg = (
                        f'Bad status code in genre {genre.upper()},'
                        + f'rank {rank}-{rank + STEP}'
                    )
                    print(msg)
            except Exception as e:
                msg = (
                    f'Exception in genre {genre.upper()},'
                    + f'rank {rank}-{rank + STEP}'
                    + f'with message: {e}'
                )
                print(msg)

        wait(self.min_delay, self.max_delay)

        return genre_id

    def collect_id(self, save_dir: str, version: int) -> None:
        """[summary]

        Args:
            save_path (str): [description]
            version (int): [description]

        Returns:
            [type]: [description]
        """
        for genre in tqdm(self.genres):
            url = URL_TEMPLATE.format(genre, 1)
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    max_titles = self._get_num_of_movies(response.content)
            except Exception as e:
                msg = (
                    f'Exception in finding max titles with message: {e}.'
                    + f'Genre {genre.upper()} skipped'
                )
                print(msg)

            wait(self.min_delay, self.max_delay)

            genre_id = self._collect_genre_id(genre, max_titles)

            filename = f'{genre.upper()}__{len(genre_id)}_v{version}'
            filepath = get_filepath(save_dir, filename)
            dump_obj(genre_id, filepath)

            msg = f'Collected {len(genre_id)} IDs in {genre.upper()} genre'
            print(msg)
