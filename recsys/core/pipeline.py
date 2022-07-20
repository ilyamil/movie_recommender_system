from typing import Tuple, Callable


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
            self._pipeline.append({
                'step_name': step[0], 'step_func': step[1]
            })

    def print_schema(self):
        print('Pipeline schema:')
        for num, step in enumerate(self._pipeline):
            name, func = step.values()
            print(
                f'{num + 1}. Name: {name:<5}',
                f'Transformer: {func}', sep='\n   '
            )

    def compose(self, data):
        result = data
        for step in self._pipeline:
            step_func = step['step_func']
            result = step_func(result)
        return result
