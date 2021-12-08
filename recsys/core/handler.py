from typing import Dict, Any
from importlib import import_module
from recsys.utils import create_logger
from recsys.core.model import Model


class RequestHandler:
    def __init__(self, model_cfg: Dict[str, Any],
                 logger_cfg: Dict[str, Any]) -> None:
        self._logger = create_logger(logger_cfg, model_cfg['log_file'])
        self._logger.info('Setting up system components...')

        model_module, model_class = model_cfg['model'].rsplit('.', 1)
        model_params = model_cfg['model_params']
        model = getattr(import_module(model_module), model_class)
        if model_params:
            model_configured = model(model_params)
        else:
            model_configured = model()

        prep_module, prep_class = model_cfg['preprocessor'].rsplit('.', 1)
        preprocessor_params = model_cfg['preprocessor_params']
        preprocessor = getattr(import_module(prep_module), prep_class)
        if preprocessor_params:
            preprocessor_configured = preprocessor(preprocessor_params)
        else:
            preprocessor_configured = preprocessor()

        post_module, post_class = model_cfg['postprocessor'].rsplit('.', 1)
        postprocessor_params = model_cfg['postprocessor_params']
        postprocessor = getattr(import_module(post_module), post_class)
        if postprocessor_params:
            postprocessor_configured = postprocessor(postprocessor_params)
        else:
            postprocessor_configured = postprocessor()

        self._configured_model = Model(model_configured,
                                       preprocessor_configured,
                                       postprocessor_configured)
        # self._metadata_loader = model_cfg['metadata_loader']
        # self._metadata_path = model_cfg['metadata_path']

        self._logger.info(
            'All components are set up successfully.'
            f' Using model {model},'
            f' preprocessor {preprocessor}, '
            f' postprocessor {postprocessor}'
        )

    def handle_request(self, request):
        pass
