from typing import Any
from abc import ABCMeta, abstractmethod


class AbstractDataTransformer(metaclass=ABCMeta):
    @abstractmethod
    def transform(self, data: Any, **params) -> Any:
        pass


class AbstractDataLoader(metaclass=ABCMeta):
    def __init__(self, source: Any):
        self._source = source

    @abstractmethod
    def load_data(self) -> Any:
        pass


class IdentityDataTransformer(AbstractDataTransformer):
    def transform(self, data: Any) -> Any:
        return data
