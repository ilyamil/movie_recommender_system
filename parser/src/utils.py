import requests
import pickle
from bs4 import BeautifulSoup


def get_soup(url):
    page = requests.get(url)
    return BeautifulSoup(page.content, 'html.parser')


def dump_obj(obj, path):
    with open(path, 'wb') as output_file:
        try:
            pickle.dump(obj, output_file)
        except Exception as e:
            print(e)


def load_obj(path):
    with open(path, 'rb') as input_file:
        try:
            return pickle.load(input_file)
        except Exception as e:
            print(e)
