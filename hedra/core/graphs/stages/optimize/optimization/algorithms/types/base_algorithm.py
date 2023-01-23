import math
import asyncio
import psutil
from hedra.core.personas.types.default_persona.default_persona import DefaultPersona
from hedra.core.personas.batching.param_type import ParamType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.personas.batching.batch import Batch
from typing import Dict, List, Tuple, Union


class BaseAlgorithm:

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int, Config]]
    ) -> None:
        self.stage_config: Config = config.get('stage_config')
        self.max_iter = config.get('iterations', 10)
        self.params = config.get('params', {})
        self.time_limit = config.get('time_limit', 60)
        self.persona_total_time = self.stage_config.total_time
        self.batch = Batch(self.stage_config)
        self.current_params = {}
        self.session = None

        self.batch_time = self.time_limit/self.max_iter

        self.param_names = []
        self.bounds = []

        self.param_values: Dict[str, Union[float, int]] = self.get_params()
        self.current_params = {}

        optimize_params = [
            param_name for param_name in self.params.keys() if param_name in self.param_values
        ]

        if len(optimize_params) < 1:
            optimize_params = [
                'batch_size'
            ]

        for param_name in optimize_params:
            min_range, max_range = self.params.get(param_name)
            param = self.param_values.get(param_name, {})
            value = param.get('value')
            params_type = param.get('type')

            self.param_names.append(param_name)

            if max_range <= min_range:
                raise Exception(
                    f'Err. - maximum value of optimization parameter {param_name} must be greater than minimum value.'
                )

            if params_type == ParamType.INTEGER:
                min_range = math.floor(min_range * value)
                max_range = math.ceil(max_range * value)

            else:
                min_range = min_range * value
                max_range = max_range * value
            
            self.bounds.append((
                min_range,
                max_range
            ))

            self.current_params[param_name] = value
            
        self.fixed_iters = False
        self.iters = 0

    def get_params(self):
        return self.batch.to_params()

    def update_params(self, persona: DefaultPersona) -> DefaultPersona:

        persona.total_time = self.batch_time

        batch_size = int(self.current_params.get(
            'batch_size',
            persona.batch.size
        ))

        batch_interval = float(self.current_params.get(
            'batch_interval',
            persona.batch.interval
        ))

        batch_gradient = float(self.current_params.get(
            'batch_gradient',
            persona.batch.gradient
        ))

        for hook in persona._hooks:

            if batch_size <= psutil.cpu_count():
                batch_size = 1000

            hook.session.pool.size = batch_size
            hook.session.sem = asyncio.Semaphore(batch_size)
            hook.session.pool.connections = []
            hook.session.pool.create_pool()

        persona.batch.size = batch_size
        persona.batch.interval = batch_interval
        persona.batch.gradient = batch_gradient

        return persona

    def optimize(self, func):
        raise NotImplementedError(
            'Err. - Base Algorithm is an abstract class and its optimize method should be overridden.'
        )