import asyncio
import math
import time
from typing import Any, Dict
from easy_logger import Logger
from async_tools.datatypes import AsyncList
from hedra.core.personas.batching.param_type import ParamType
from .algorithms import get_algorithm


class Optimizer:

    def __init__(self, config: Dict[str, Any]) -> None:
        self.stage_name = config.get('stage_name')
        self.actions = AsyncList()

        algorithm_type = config.get('algorithm', 'shg')
        persona = config.get('persona')


        logger = Logger()
        self.algorithm_logger = logger.generate_logger()

        self._optimization_time_limit = config.get('time_limit', 60)

        self.algorithm = get_algorithm(
            algorithm_type,
            {
                **config,
                'persona': persona
            }
        )

        self._current_iter = 0
        self.optimized_results = {}
        self._max_aps = 0
        self._event_loop: asyncio.AbstractEventLoop = None

    def optimize(self, loop):
        self._event_loop = loop

        results = None

        start = time.time()
        
        results = self.algorithm.optimize(self._run_optimize)

        self.total_optimization_time = time.time() - start

        self.algorithm.persona.total_time = self.algorithm.persona_total_time

        self.optimized_results = {
            'optimized_batch_size': int(results.x[0]),
            'optimization_iters': self.algorithm.max_iter,
            'optimization_iter_duation': self.algorithm.batch_time,
            'optimization_total_time': self.total_optimization_time,
            'optimization_max_aps': self._max_aps
        }

        return self.optimized_results

    async def _optimize(self, *params):

        self.current_params = {}
        for idx, param in enumerate(params):
            param_name = self.algorithm.param_names[idx]
            self.current_params[param_name] = param

        if self._current_iter < self.algorithm.max_iter:

            self.algorithm.update_params()

            try:
                completed = await asyncio.wait_for(
                    self.algorithm.persona.execute(), 
                    timeout=self.algorithm.batch_time * 2
                )

                completed_count = len([complete for complete in completed if complete.error is None])
            except asyncio.TimeoutError:
                completed_count = 1

            elapsed = self.algorithm.persona.end - self.algorithm.persona.start
       
            if completed_count < 1:
                completed_count = elapsed

            return elapsed/completed_count

        return 0

    def _run_optimize(self, xargs):

        for idx, param in enumerate(xargs):
            param_name = self.algorithm.param_names[idx]
            param = self.algorithm.param_values.get(param_name, {})
            param_type = param.get('type')

            if param_type == ParamType.INTEGER:
                xargs[idx] = int(xargs[idx])

        inverse_actions_per_second = self._event_loop.run_until_complete(
            self._optimize(*xargs)
        )

        self._current_iter += 1


        return inverse_actions_per_second