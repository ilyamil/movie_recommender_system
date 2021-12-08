from typing import Any
from recsys.core.model import AbstractModel


TITLE_ID = '/title/tt0068646/'


class TheGodfatherModel(AbstractModel):
    def infer(self, data: Any = None):
        return TITLE_ID
