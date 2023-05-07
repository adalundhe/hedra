import math
import asyncio
import psutil
from hedra.core.personas.types.default_persona.default_persona import DefaultPersona
from hedra.core.personas.batching.param_type import ParamType
from hedra.core.engines.client.config import Config
from hedra.core.personas.batching.batch import Batch
from hedra.core.graphs.stages.optimize.optimization.parameters.parameter import Parameter
from typing import (
    Dict, 
    List, 
    Tuple, 
    Union, 
    Optional
)


class BaseAlgorithm:

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int, Config]],
        distribution_idx: int=None
    ) -> None:
        self.stage_config: Config = config.get('stage_config')
        self.max_iter = config.get('iterations', 10)
        self.distribution: Optional[List[int]] = None

        if self.stage_config.experiment:
            self.distribution = self.stage_config.experiment.get('distribution')

        parameters: List[Parameter] = config.get('params', [])
        algorithm_parameters: Dict[str, Parameter] = {}

        for parameter in parameters:
            algorithm_parameters[parameter.parameter_name] = parameter

        self.params: Dict[str, Parameter] = algorithm_parameters
        self.time_limit = config.get('time_limit', 60)
        self.persona_total_time = self.stage_config.total_time
        self.batch = Batch(self.stage_config)
        self.current_params = {}
        self.session = None
        self.distribution_idx: Optional[int] = distribution_idx
        self._boundaries_by_param_name = {}

        if self.max_iter is None or self.max_iter <= 0:
            self.max_iter = 10

        if self.time_limit is None or self.time_limit <= 0:
            self.time_limit = 60

        self.batch_time = self.time_limit/self.max_iter

        self.param_names = []
        self.bounds = []

        self.param_values: Dict[str, Union[float, int]] = self.get_params()
        self.current_params = {}

        self.optimize_params = [
            param_name for param_name in self.params.keys() if param_name in self.param_values
        ]

        if len(self.optimize_params) < 1:
            self.optimize_params = [
                'batch_size'
            ]

        for param_name in self.optimize_params:
            parameter = self.params.get(param_name)
                    
            param = self.param_values.get(param_name, {})
            value = param.get('value')
            params_type = param.get('type')

            if self.distribution_idx is not None:
                parameter.minimum = 0.01
                parameter.maximum = 10
                value = self.distribution[self.distribution_idx]
                param['value'] = value

            min_range = parameter.minimum
            max_range = parameter.maximum

            self.param_names.append(param_name)

            if parameter.feed_forward:
                if params_type == ParamType.INTEGER:
                    min_range = math.floor(min_range * value)
                    max_range = math.ceil(max_range * value)

                else:
                    min_range = min_range * value
                    max_range = max_range * value

            boundaries = (min_range, max_range)
            self.bounds.append(boundaries)
            self._boundaries_by_param_name[param_name] = boundaries

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