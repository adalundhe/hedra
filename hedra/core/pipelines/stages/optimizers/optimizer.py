import asyncio
import time
from typing import Any, Dict
from easy_logger import Logger
from alive_progress import alive_bar
from async_tools.functions import awaitable
from async_tools.datatypes import AsyncList
from .types import (
    SHGOptimizer,
    DualAnnealingOptimizer,
    DifferentialEvolutionOptimizer
)


class Optimizer:

    optimizer_types = {
        'shg': SHGOptimizer,
        'dual-annealing': DualAnnealingOptimizer,
        'diff-evolution': DifferentialEvolutionOptimizer
    }

    def __init__(self, config: Dict[str, Any]) -> None:
        
        self.persona = config.get('persona')
        self.actions = AsyncList()
        self.iterations = config.get('iterations')

        self.algorithm = config.get('algorithm')
        if self.algorithm is None:
            self.algorithm = 'shg'

        logger = Logger()
        self.session_logger = logger.generate_logger()

        self._optimization_time_limit = config.get('time_limit', 60)

        self.batch_time = self.persona.total_time/self.iterations
        self.persona_total_time = self.persona.total_time
        self.persona.total_time = self.batch_time

        self._target_aps = []
        self._actual_aps = []

        self.initial_batch_size = self.persona.batch.size
        self.batch_max_size = int(self.persona.batch.size * 2)
        self.batch_min_size = int(self.persona.batch.size * 0.5)
        self.batch_min_interval = 0

        self.optimizer = self.optimizer_types.get(self.algorithm)(
            [
                (self.batch_min_size, self.batch_max_size)
            ],
            max_iter=self.iterations
        )
        
        self._current_iter = 0
        self.optimized_results = {}
        self._max_aps = 0

    async def optimize(self):
        self._event_loop = asyncio.get_event_loop()

        results = None

        start = time.time()
        
        results = await self.optimizer.optimize(self._run_optimize)

        self.total_optimization_time = time.time() - start

        self.persona.total_time = self.persona_total_time

        self.optimized_results = {
            'optimized_batch_size': int(results.x[0]),
            'optimization_iters': self.iterations,
            'optimization_iter_duation': self.batch_time,
            'optimization_total_time': self.total_optimization_time,
            'optimization_max_aps': self._max_aps
        }

        return self.optimized_results

    async def _optimize(self, batch_size):
        if self._current_iter < self.iterations:
            for hook in self.persona._hooks:
                hook.session.concurrency = batch_size
                hook.session.pool.size = batch_size
                hook.session.sem = asyncio.Semaphore(batch_size)
                hook.session.pool.connections = []
                hook.session.pool.create_pool()

            completed = await self.persona.execute()
            completed_count = len([complete for complete in completed if complete.error is None])
            elapsed = self.persona.end - self.persona.start
       
            if completed_count < 1:
                completed_count = elapsed

            return elapsed/completed_count

        return 0

    def _run_optimize(self, xargs):
        batch_size = xargs

        optimization = asyncio.run_coroutine_threadsafe(self._optimize(int(batch_size)), self._event_loop)
        inverse_actions_per_second = optimization.result()

        self._current_iter += 1


        return inverse_actions_per_second