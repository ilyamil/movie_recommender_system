from abc import ABCMeta, abstractmethod
from copy import copy
from typing import Any, Iterable, Tuple, Callable
from recsys.core.data import (AbstractDataTransformer,
                              IdentityDataTransformer)


class AbstractModel(metaclass=ABCMeta):
    def __init__(self, **params):
        pass

    @abstractmethod
    def infer(self, data: Any) -> Iterable[str]:
        pass


class Pipeline:
    def __init__(self, *steps: Tuple[str, Callable]):
        self._pipeline = []
        for step_num, step in enumerate(steps):
            if len(step) == 0:
                raise AttributeError(
                    f'Step #{step_num + 1} is empty!'
                )
            if len(step) != 2:
                raise AttributeError(
                    'Each step must be of length 2'
                    ' and match a form (<step_name>, <step_class>)'
                )
            self._pipeline.append({'step_name': step[0],
                                   'step_func': step[1]})

    @property
    def schema(self):
        print('Pipeline schema:')
        for num, step in enumerate(self._pipeline):
            name, func = step.values()
            print(f'{num + 1}. Name: {name:<5}',
                  f'Transformer: {func}', sep='\n   ')

    def compose(self, data):
        result = copy(data)
        for step in self._pipeline:
            step_func = step['step_func']
            result = step_func(result)
        return result


class Model:
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
