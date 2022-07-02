import asyncio
import math
import json
from re import S
import time
from unittest import result
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

    def __init__(self, persona) -> None:
        
        self.persona = persona
        self.actions = AsyncList()
        self.config = persona.config
        self._is_parallel = persona.is_parallel

        logger = Logger()
        self.session_logger = logger.generate_logger()

        self._persona_total_time = persona.total_time

        if persona.total_time <= 60:
            self.batch_max_time = persona.total_time
        else:
            self.batch_max_time = 60

        self._target_aps = []
        self._actual_aps = []

        # if self.batch_max_time > 60:
        #     self.session_logger.warning('Warning - provided iter duration greater than 60 sec. Setting duratin to 60 sec.')
        #     self.batch_max_time = 60

        # elif self.batch_max_time < 1:
        #     self.batch_max_time = 30

        self.batch_min_time = 1

        self.batch_max_size = int(persona.batch.size * 10)
        self.batch_min_size = int(persona.batch.size * 0.5)
        self.batch_min_interval = 0

        if persona.total_time > 60:
            self.batch_max_interval = 6
        else:
            self.batch_max_interval = math.ceil(persona.total_time * 0.1)

        self.optimize_iters = self.config.get('optimize', 0)
        self.optimizer_type = self.config.get('optimizer_type', 'shg')

        self.optimizer = self.optimizer_types.get(self.optimizer_type)(
            [
                (self.batch_min_size, self.batch_max_size),
                (self.batch_min_interval, self.batch_max_interval)
            ],
            max_iter=self.optimize_iters
        )
        
        self.save_optimized_path = self.config.get('save_optimized')
        self._current_iter = 1
        self.progress_bar = alive_bar
        self.active_bar = None
        self.optimized_results = {}
        self.total_optimization_time = 0
        self._no_run_visuals = self.config.get('no_run_visuals', False)
        self._max_aps = 0


    @classmethod
    def about(cls):

        optimization_algorithms = '\n\t'.join([f'- {optimizer_type}' for optimizer_type in cls.optimizer_types.keys()])

        return f'''
        Optimization

        key arguments:

        --optimize <optimize_iterations>

        --optimizer-type (optional) <optimization_algorithm>

        --optimizer-iter-duration (optional) <max_iter_duration_seconds>

        --save-optimized <filepath_to_json_file_to_save_optimization_results>

        Optimizers are a unique feature to Hedra, allowing you to utilize global optimization algorithms to perform
        stochastic testing, automate performance profiling of targets, and determine optimial batch size and batch time
        parameters to maximize Hedra's performance. Optimization is designed to be easy to use, basing parameter
        search off of --total-time, specified (or default) --batch-size, and the additional parameters noted
        above. All optimizers share a single method:

        - optimize (executes optimization using the specified optimizer/algorithm for the specified number of iterations)
        
        Optimization algorithms currently supported include:

        {optimization_algorithms}

        For more information on each algorithm, run the command:

            hedra --about optimization:<algorithm>


        Related Topics:

         - batches
         - personas
        
        '''

    async def optimize(self):
        self._event_loop = asyncio.get_running_loop()

        results = None

        start = time.time()
        
        if self._no_run_visuals is False and self._is_parallel is False:
            with self.progress_bar(
                self.optimize_iters, 
                title='Optimizing', 
                bar=None,
                spinner='dots_waves2',
                stats=False,
                monitor=self.optimizer.fixed_iters
            ) as bar:
                self.active_bar = bar
                results = await self.optimizer.optimize(self._run_optimize)

        else:
            results = await self.optimizer.optimize(self._run_optimize)

        self.total_optimization_time = time.time() - start

        self.persona.duration = self._persona_total_time
        
        self.optimized_results = {
            'optimized_batch_size': int(results.x[0]),
            'optimized_batch_interval': results.x[1],
            'optimization_iters': self.optimize_iters,
            'optimization_iter_duation': self.batch_max_time,
            'optimization_total_time': self.total_optimization_time,
            'optimization_max_aps': self._max_aps
        }

        self.persona.total_time = self._persona_total_time

        if self.save_optimized_path:
            with open(self.save_optimized_path) as params_file:
                json.dump(params_file, self.optimized_results, indent=4)

        return self.optimized_results

    async def _optimize(self, batch_size, batch_interval):
        completed = []

        if self.persona.total_time > 60:
            self.persona.total_time = 60

        if self._current_iter <= self.optimize_iters:

            self.persona.batch.size = batch_size
            self.persona.batch.interval.wait_period = batch_interval
            
            completed = await self.persona.execute()
            completed_count = len(completed)

            elapsed = self.persona.end - self.persona.start
            actions_per_section = completed_count/elapsed

            if actions_per_section > self._max_aps:
                self._max_aps = actions_per_section

            if self._no_run_visuals is False and self._is_parallel is False:
                await awaitable(
                    self.active_bar
                )

            return actions_per_section

        return 0

    def _run_optimize(self, xargs):
        batch_size, batch_interval = xargs
        optimization = asyncio.run_coroutine_threadsafe(self._optimize(int(batch_size), batch_interval), self._event_loop)
        actions_per_second = optimization.result()

        self._target_aps.append(self._max_aps)
        self._actual_aps.append(actions_per_second)
        # error = math.sqrt(sum([target_aps - actual_aps for target_aps, actual_aps in zip(self._target_aps, self._actual_aps)])**2/self._current_iter)
        self._current_iter += 1


        return actions_per_second * -1 