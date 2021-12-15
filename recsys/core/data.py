"""
Module contains interfaces for data manipulations.
"""
from typing import Any, Union
from collections.abc import Iterable
from abc import ABCMeta, abstractmethod
from pandas import read_csv, concat


class AbstractDataLoader(metaclass=ABCMeta):
    def __init__(self, path: Union[Any, Iterable[Any]], **kwargs):
        if isinstance(path, str) or not isinstance(path, Iterable):
            self._path = [path]
        else:
            self._path = path
        self._kwargs = kwargs

    @abstractmethod
    def load_data(self, **kwargs) -> Any:
        pass


class CSVDataLoader(AbstractDataLoader):
    def __init__(self, path: Union[Any, Iterable[Any]], **kwargs):
        super().__init__(path, **kwargs)

    def load_data(self, union: bool = False, **union_kwargs) -> Any:
        data = (read_csv(p, **self._kwargs) for p in self._path)
        if union:
            return concat(data, **union_kwargs)
        return data


class AbstractDataTransformer(metaclass=ABCMeta):
    @abstractmethod
    def transform(self, data: Any, **params) -> Any:
        pass


class IdentityDataTransformer(AbstractDataTransformer):
    def transform(self, data: Any) -> Any:
        return data
