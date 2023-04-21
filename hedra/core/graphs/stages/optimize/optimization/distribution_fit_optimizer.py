import asyncio
import uvloop
from hedra.core.engines.client.config import Config
from hedra.core.personas.batching.param_type import ParamType
from hedra.core.personas.types.approximate_distribution import ApproximateDistributionPersona
from typing import Dict, Any, List, Union
from .optimizer import Optimizer, cancel_pending


class DistributionFitOptimizer(Optimizer):
    
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

        self.distribution = self.stage_config.experiment.get('distribution')
        self.algorithm.batch_time = self.stage_config.total_time

    def _setup_persona(self, stage_config: Config):

        persona = ApproximateDistributionPersona(self.stage_config)
        persona.collect_analytics = True
        persona.optimization_active = True
        persona.setup(self.stage_hooks, self.metadata_string)

        return persona

    async def _optimize(self, xargs: List[Union[int, float]]):

        if self._current_iter < self.algorithm.max_iter and self.elapsed < self._optimization_time_limit:

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
            
            persona = self.algorithm.update_params(persona)

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter}')

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Size - {persona.batch.size}')
            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Interval - {persona.batch.interval}')
            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Batch Gradient - {persona.batch.gradient}')

            completed_count = 0
            try:
                results = await asyncio.wait_for(
                    persona.execute(),
                    timeout=persona.total_time * 2
                )

                completed_count = len([result for result in results if result.error is None])

            except Exception:
                pass

            elapsed = persona.end - persona.start

            # print(persona.streamed_analytics.interval_completion_rates)

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - took - {round(elapsed, 2)} - seconds')
       
            if completed_count < 1:
                completed_count = 1

            if elapsed < 1:
                elapsed = float('inf')

            # This section necessary for low-bandwidth connections
            # where the condensed runtime of optimize runs can
            # result
            all_tasks = asyncio.all_tasks()
            running_task = asyncio.current_task()
            for task in all_tasks:
                if task != running_task and task.cancelled() is False:
                    try:
                        task.cancel()
                        if task.cancelled() is False:
                            await task
                    
                    except asyncio.CancelledError:
                        pass
                    except asyncio.InvalidStateError:
                        pass
                    except ConnectionResetError:
                        pass

            await self.logger.filesystem.aio['hedra.optimize'].debug(f'{self.metadata_string} - Optimizer iteration - {self._current_iter} - Inverted APS score- {elapsed/completed_count}')
            
            return elapsed/completed_count