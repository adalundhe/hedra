import asyncio
import dill
import functools
import signal
import psutil
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Callable, 
    Awaitable, 
    Any, 
    Optional, 
    Dict, 
    List
)
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.reporting.metric.custom_metric import CustomMetric


RawResultsSet = Dict[str, ResultsSet]
def handle_loop_stop(
        signame, 
        executor: ThreadPoolExecutor
    ):
    try:
        executor.shutdown(wait=False, cancel_futures=True)

    except BrokenPipeError:
        pass

    except RuntimeError:
        pass

class MetricHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        metric_type: str,
        group: Optional[str] = None,
        order: int=1,
        skip: bool=False
    ) -> None:        
        super().__init__(
            name, 
            shortname, 
            call, 
            order=order,
            skip=skip,
            hook_type=HookType.METRIC
        )
        
        self.names = []
        self.metric_type = metric_type
        self.group = group
        self.order = order
        self.executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=True))
        self.loop: asyncio.AbstractEventLoop = None

    async def call(self, **kwargs):

        if self.skip:
            return kwargs
        
        results: RawResultsSet = self.context['analyze_stage_raw_results']
        self.loop = asyncio.get_running_loop()
        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self.loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self.executor
                )
            )


        args_by_hook = {}

        if 'results' in self.params:
            stage_results = await self.loop.run_in_executor(
                self.executor,
                functools.partial(
                    self._generate_deserialized_results,
                    results
                )
            )
            
            for result_set_name, results_set in stage_results.items():
                args_by_hook[result_set_name] =  {
                    **kwargs,
                    'results': results_set
                }

        custom_metric_results: List[Dict[str, CustomMetric]] = await asyncio.gather(*[
            asyncio.create_task(
                self._calculate_stage_custom_metric(
                    args_by_hook[set_name],
                    set_name
                )
            ) for set_name in args_by_hook.keys()
        ])

        metric_results = {}

        for custom_metric in custom_metric_results:
            metric_results.update(custom_metric)

        self.executor.shutdown(wait=False, cancel_futures=True)

        return {
            **kwargs,
            **metric_results
        }
    
    async def _calculate_stage_custom_metric(
            self,
            hook_kwargs: Dict[str, Any],
            set_name: Optional[str]=None
    ) -> Dict[str, CustomMetric]:

        metric_result = await self._call(**{
            name: value for name, value in hook_kwargs.items() if name in self.params
        })
        
        if isinstance(metric_result, dict):
            for metric_name, metric_value in metric_result.items():

                assert isinstance(metric_name, str)
                assert isinstance(metric_value, (float, int))

                metric_fullname = f'{set_name}_{metric_name}'

                custom_metric = CustomMetric(
                    metric_fullname,
                    metric_name,
                    metric_value,
                    metric_group=set_name,
                    metric_type=self.metric_type
                )

                metric_result[metric_name] = custom_metric
                self.context[metric_name] = custom_metric

            return metric_result
        
        assert isinstance(metric_result, (float, int))

        metric_fullname = f'{set_name}_{self.shortname}'
        custom_metric = CustomMetric(
                metric_fullname,
                self.shortname,
                metric_result,
                metric_group=set_name,
                metric_type=self.metric_type
        
        )

        self.context[metric_fullname] = custom_metric
        
        return {
            metric_fullname: custom_metric 
        }

    def _generate_deserialized_results(self, results: RawResultsSet) -> Dict[str, List[BaseResult]]:
        stage_results: Dict[str, List[BaseResult]] = defaultdict(list)

        for results_set in results.values():
            for result in results_set.results:
                stage_result: BaseResult = dill.loads(result)
                stage_results[stage_result.name].append(stage_result)

        return stage_results


    def copy(self):
        metric_hook = MetricHook(
            self.name,
            self.shortname,
            self._call,
            self.group,
            order=self.order,
            skip=self.skip,
        )

        metric_hook.stage = self.stage

        return metric_hook