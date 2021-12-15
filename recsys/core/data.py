"""
Module contains interfaces for data manipulations.
"""
import os
from typing import Any, Union
from collections.abc import Iterable
from abc import ABCMeta, abstractmethod
from pandas import read_csv, concat


class AbstractDataLoader(metaclass=ABCMeta):
    def __init__(self, path: Union[Any, Iterable[Any]], **kwargs):
        if isinstance(path, str):
            if os.path.isdir(path):
                self._path = os.listdir(path)
            elif os.path.exists(path):
                self._path = [path]
            else:
                raise ValueError(f'Path {path} does not exists or empty')
        elif isinstance(path, Iterable):
            nonexistent_paths = [p for p in path if os.path.isfile(p)]
            if len(nonexistent_paths) > 0:
                fmt_paths = "\n".join(nonexistent_paths)
                raise ValueError(f'These files do not exist: {fmt_paths}')
            else:
                self._path = path

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
