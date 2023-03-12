import asyncio
from collections import defaultdict
from typing import Callable, Awaitable, Any, Optional, Dict, List
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.reporting.metric.custom_metric import CustomMetric


RawResultsSet = Dict[str, ResultsSet]


class MetricHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        metric_type: str,
        group: Optional[str] = None,
        order: int=1
    ) -> None:        
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.METRIC
        )
        
        self.names = []
        self.metric_type = metric_type
        self.group = group
        self.order = order

    async def call(self, **kwargs):

        results: RawResultsSet = self.context['analyze_stage_raw_results']

        stage_results: Dict[str, List[BaseResult]] = defaultdict(list)
        args_by_hook = {}

        if 'results' in self.params:
            for results_set in results.values():
                for result in results_set.results:
                    stage_results[result.name].append(result)

            
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


        

    def copy(self):
        metric_hook = MetricHook(
            self.name,
            self.shortname,
            self._call,
            self.group,
            order=self.order
        )

        metric_hook.stage = self.stage

        return metric_hook