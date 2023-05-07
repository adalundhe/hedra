import asyncio
import time
import signal
from collections import defaultdict
from hedra.core.experiments.variant import Variant
from hedra.core.engines.client.config import Config
from hedra.core.personas.batching.param_type import ParamType
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.versioning.flags.types.unstable.flag import unstable
from statistics import mean
from typing import (
    Dict, 
    Any, 
    List, 
    Union, 
    Tuple
)
from .algorithms.types.point_optimizer import PointOptimizer
from .optimizer import Optimizer


@unstable
class DistributionFitOptimizer(Optimizer):
    
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        self.optimization_start = 0
        self.algorithm: PointOptimizer = None

        self.variant_weight = self.stage_config.experiment.get('weight')
        self.distribution_intervals = self.stage_config.experiment.get('intervals')
        self.distribution_type = self.stage_config.experiment.get('distribution_type')
        self.distribution = self.stage_config.experiment.get('distribution')

        self.variant = Variant(
            self.stage_name,
            weight=self.variant_weight,
            distribution=self.distribution_type,
        )

        self.algorithms: List[PointOptimizer] = []
        self.target_interval_completions: int = 0
        self.completion_rates = defaultdict(list)

        for distribution_idx in range(len(self.distribution)):
            self.algorithms.append(PointOptimizer(
                {
                    **config,
                    'stage_config': self.stage_config
                },
                distribution_idx=distribution_idx
            ))
            

    def _setup_persona(self, stage_config: Config) -> DefaultPersona:
        persona = DefaultPersona(stage_config)
        persona.setup(self.stage_hooks, self.metadata_string)

        return persona
    
    def optimize(self) -> Dict[str, Union[int, float]]:

        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)

        def handle_loop_stop(signame):
            try:
                self._event_loop.close()

            except BrokenPipeError:
                pass
                
            except RuntimeError:
                pass

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):

            self._event_loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(signame)
            )

        self._event_loop.set_exception_handler(self._handle_async_exception)

        optimization_results = self._event_loop.run_until_complete(
            self._optimize_async()
        )

        self._event_loop.close()

        return optimization_results

    
    async def _optimize_async(self) -> Dict[str, Union[int, float]]:

        results = None

        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Starting optimization')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Time Limit - {self._optimization_time_limit}')
        self.start = 0

        distribution_optimized_params = []
        distribution_mean_errors = []

        self.optimization_start = time.time()

        for distribution_value, algorithm in zip(self.distribution, self.algorithms):
            self.completion_rates = defaultdict(list)
            self.algorithm: PointOptimizer = algorithm
            self.algorithm.batch_time = self.stage_config.total_time/self.distribution_intervals

            self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Algorithm - {self.algorithm_type}')
            self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Batch Time - {self.algorithm.batch_time}')
            self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization config: Max Iter - {self.algorithm.max_iter}')


            self.target_interval_completions = distribution_value

            self.elapsed = 0
            self._current_iter = 0
            self.start = time.time()

            results: Dict[str, Dict[str, Union[int, float]]] = await self.algorithm.optimize(self._optimize_iteration)

            optimization_results = results.get('batch_size')
            optimized_distribution_value = optimization_results.get('minimized_distribution_value')
            optimized_error_mean = optimization_results.get('minimized_error_mean')


            distribution_optimized_params.append(optimized_distribution_value)   
            distribution_mean_errors.append(optimized_error_mean)     
        
        self.total_optimization_time = time.time() - self.optimization_start

        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization took - {round(self.total_optimization_time, 2)} - seconds')
        self.logger.filesystem.sync['hedra.optimize'].info(f'{self.metadata_string} - Optimization - max actions per second - {self._max_aps}')

        self.optimized_results = {
            'optimized_distribution': distribution_optimized_params,
            'optimization_mean_error': mean(distribution_mean_errors),
            'optimization_iters': self.algorithm.max_iter,
            'optimization_iter_duation': self.algorithm.batch_time,
            'optimization_total_time': self.total_optimization_time,
            'optimization_max_aps': self._max_aps
        }

        self.total_optimization_time = time.time() - self.start

        return self.optimized_results

    async def _optimize_iteration(self, xargs: List[Union[int, float]]) -> Tuple[Union[None, float], int]:

        if self.elapsed > self.algorithm.time_limit:
            return None, self.target_interval_completions

        persona = self._setup_persona(self.stage_config)

        for idx, param in enumerate(xargs):
            param_name = self.algorithm.param_names[idx]
            param = self.algorithm.param_values.get(param_name, {})
            param_type = param.get('type')

            if param_type == ParamType.INTEGER:
                xargs[idx] = int(xargs[idx])

            else:
                xargs[idx] = float(xargs[idx])

            param['value'] = xargs[idx]

            self.current_params[param_name] = xargs[idx]
            self.algorithm.current_params[param_name] = xargs[idx]

        persona.total_time = self.algorithm.batch_time
        persona = self.algorithm.update_params(persona)
        await persona.set_concurrency(persona.batch.size)

        await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter}')

        await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Size - {persona.batch.size}')
        await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Interval - {persona.batch.interval}')
        await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Gradient - {persona.batch.gradient}')

        completed_count = 0

        try:
            results = await persona.execute()
            completed_count = len([result for result in results if result.error is None])

        except Exception:
            pass

        elapsed = persona.end - persona.start

        await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - took - {round(elapsed, 2)} - seconds')
    
        self.completion_rates[persona.batch.size].append(completed_count)
        error = completed_count - self.target_interval_completions

        await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Target error- {error}')

        self._current_iter += 1
        self.elapsed = time.time() - self.start

        if completed_count < 1:
            return -(self.base_batch_size**2),  self.target_interval_completions
        
        return error, self.target_interval_completions
