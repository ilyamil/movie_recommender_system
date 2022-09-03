from typing import List


class Matcher:
    def __init__(self, titles: List[str], **params):
        self.titles = titles

    def match(self, user_input: List[str]):
        pass
