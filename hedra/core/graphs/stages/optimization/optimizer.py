import asyncio
import uuid
import time
import threading
import os
from typing import Any, Dict
from hedra.core.personas.batching.param_type import ParamType
from hedra.logging import HedraLogger
from hedra.tools.data_structures import AsyncList
from .algorithms import get_algorithm


class Optimizer:

    def __init__(self, config: Dict[str, Any]) -> None:

        self.logger = HedraLogger()
        self.logger.initialize()

        self.optimizer_id = str(uuid.uuid4())
        self.thread_id = threading.current_thread().ident
        self.process_id = os.getpid()
        
        self.graph_name = config.get('graph_name')
        self.graph_id = config.get('graph_id')
        self.source_stage_name = config.get('source_stage_name')
        self.source_stage_id = config.get('source_stage_id')

        self.metadata_string = f'Graph - {self.graph_name}:{self.graph_id} - thread:{self.thread_id} - process:{self.process_id} - Stage: {self.source_stage_name}:{self.source_stage_id} - Optimizer: {self.optimizer_id} - '

        self.stage_name = config.get('stage_name')
        self.actions = AsyncList()

        self.algorithm_type = config.get('algorithm', 'shg')
        persona = config.get('persona')

        self._optimization_time_limit = config.get('time_limit', 60)

        self.algorithm = get_algorithm(
            self.algorithm_type,
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

        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Starting optimization')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Time Limit - {self._optimization_time_limit}')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Algorithm - {self.algorithm_type}')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Batch Time - {self.algorithm.batch_time}')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Max Iter - {self.algorithm.max_iter}')

        start = time.time()
        
        results = self.algorithm.optimize(self._run_optimize)
        optimized_batch_size = int(results.x[0])

        self.total_optimization_time = time.time() - start

        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization took - {round(self.total_optimization_time, 2)} - seconds')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization - max actions per second - {self._max_aps}')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization - optimized batch size - {optimized_batch_size}')

        self.algorithm.persona.total_time = self.algorithm.persona_total_time

    
        self.optimized_results = {
            'optimized_batch_size': optimized_batch_size,
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

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter}')

            self.algorithm.update_params()

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Size - {self.algorithm.persona.batch.size}')
            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Interval - {self.algorithm.persona.batch.interval}')
            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Gradient - {self.algorithm.persona.batch.gradient}')

            try:
                completed = await asyncio.wait_for(
                    self.algorithm.persona.execute(), 
                    timeout=self.algorithm.batch_time * 2
                )

                completed_count = len([complete for complete in completed if complete.error is None])
            except asyncio.TimeoutError:
                completed_count = 1

            elapsed = self.algorithm.persona.end - self.algorithm.persona.start

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - took - {round(elapsed, 2)} - seconds')
       
            if completed_count < 1:
                completed_count = elapsed

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - actions per second - {round(completed/elapsed)}')
            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Inverted APS score- {elapsed/completed}')

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