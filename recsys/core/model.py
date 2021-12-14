from abc import ABCMeta, abstractmethod
from typing import Any, Iterable
from recsys.core.data import (AbstractDataTransformer,
                              IdentityDataTransformer)


class AbstractModel(metaclass=ABCMeta):
    def __init__(self, **params):
        pass

    @abstractmethod
    def infer(self, data: Any) -> Iterable[str]:
        pass


class AssembledModel:
    """
    Inference-ready model.
    """
    def __init__(
        self, model: AbstractModel,
        preprocessor: AbstractDataTransformer = IdentityDataTransformer(),
        postprocessor: AbstractDataTransformer = IdentityDataTransformer(),
        **params
    ) -> None:
        self._model = model
        self._preprocessor = preprocessor
        self._postprocessor = postprocessor
        self._params = params if params is not None else None

    def preprocess(self, data: Any, **params) -> Any:
        return self._preprocessor.transform(data, **params)

    def postprocess(self, data: Any, **params) -> Any:
        return self._postprocessor.transform(data, **params)

    def recommend(self, data: Any) -> Any:
        inference_ready_data = self.preprocess(data)
        raw_recomms = self._model.infer(inference_ready_data)
        formatted_recomms = self.postprocess(raw_recomms)
        return formatted_recomms
