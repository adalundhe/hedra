import math
import asyncio
from hedra.core.personas.types.default_persona.default_persona import DefaultPersona
from hedra.core.personas.batching.param_type import ParamType
from typing import Dict, List, Tuple, Union


class BaseAlgorithm:

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int]]
    ) -> None:
        self.persona: DefaultPersona = config.get('persona', DefaultPersona)
        self.max_iter = config.get('iterations', 10)
        self.params = config.get('params', {})
        self.persona_total_time = self.persona.total_time
        self.current_params = {}

        self.batch_time = self.persona.total_time/self.max_iter

        self.persona.total_time = self.batch_time

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
            params_type = param.get('type')

            self.param_names.append(param_name)

            if max_range <= min_range:
                raise Exception(
                    f'Err. - maximum value of optimization parameter {param_name} must be greater than minimum value.'
                )

            if params_type == ParamType.INTEGER:
                min_range = math.floor(min_range)
                max_range = math.ceil(max_range)

            self.bounds.append((
                min_range,
                max_range
            ))
            
        self.fixed_iters = False
        self.iters = 0

    def get_params(self):
        return self.persona.batch.to_params()

    def update_params(self):

        batch_size = self.current_params.get(
            'batch_size',
            self.persona.batch.size
        )

        batch_interval = self.current_params.get(
            'batch_interval',
            self.persona.batch.interval
        )

        batch_gradient = self.current_params.get(
            'batch_gradient',
            self.persona.batch.gradient
        )

        for hook in self.persona._hooks:
            hook.session.pool.size = batch_size
            hook.session.sem = asyncio.Semaphore(batch_size)
            hook.session.pool.connections = []
            hook.session.pool.create_pool()

        self.persona.batch.interval = batch_interval
        self.persona.batch.gradient = batch_gradient

    def optimize(self, func):
        raise NotImplementedError(
            'Err. - Base Algorithm is an abstract class and its optimize method should be overridden.'
        )