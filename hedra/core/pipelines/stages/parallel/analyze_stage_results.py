import asyncio
import numpy
from collections import defaultdict
from typing import Dict, List, Union
from hedra.reporting.events import EventsGroup
from hedra.reporting.metric import MetricsSet
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.pipelines.hooks.registry.registrar import registrar


def analyze_stage_results(
    stage_name: str, 
    custom_metric_hook_names: List[str],
    stage_results: Dict[str, Union[int, List[BaseResult]]]
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    results = loop.run_until_complete(calculate_results(
        stage_name,
        custom_metric_hook_names,
        stage_results
    ))

    loop.stop()
    loop.close()

    return results



async def calculate_results(
    stage_name: str, 
    custom_metric_hook_names: List[str],
    stage_results: Dict[str, Union[int, List[BaseResult]]]
):

    custom_metric_hooks = []
    for metric_hook_name in custom_metric_hook_names:
        custom_metric_hook = registrar.all.get(metric_hook_name)
        custom_metric_hooks.append(custom_metric_hook)
    
    events =  defaultdict(EventsGroup)
    stage_total = 0
    stage_total_time = stage_results.get('total_elapsed')

    await asyncio.gather(*[
        asyncio.create_task(events[stage_result.name].add(
                stage_result,
                stage_name
            )) for stage_result in stage_results.get('results')
    ])

    grouped_stats = {}
    
    for event_group_name, events_group in events.items():  

        custom_metrics = defaultdict(dict)
        for custom_metric in custom_metric_hooks:
            custom_metrics[custom_metric.group][custom_metric.shortname] = await custom_metric.call(events_group)

        group_total = events_group.succeeded + events_group.failed

        metrics_groups = {}
        for group_name, group_timings in events_group.timings.items():
            metrics_group = calculate_group_stats(
                group_name,
                group_timings
            )

            metrics_groups = {**metrics_groups, **metrics_group}

        metric_data = {
            'total': group_total,
            'succeeded': events_group.succeeded,
            'failed': events_group.failed,
            'actions_per_second': group_total/stage_total_time,
            'errors': list([
                {
                    'message': error_message,
                    'count': error_count
                } for error_message, error_count in events_group.errors.items()
            ]),
            'groups': metrics_groups,
            'custom': custom_metrics
        }

        metric = MetricsSet(
            event_group_name,
            events_group.events[0].source,
            stage_name,
            metric_data,
            events_group.tags
        )

        grouped_stats[event_group_name] = metric

        stage_total += group_total

    return {
        'stage_metrics': {
            stage_name: {
                'total': stage_total,
                'actions_per_second': stage_total/stage_total_time,
                'actions': grouped_stats
            }
        },
        'stage_total': stage_total
    }


def calculate_group_stats(group_name: str, data: List[Union[int, float]]):

        quantile_ranges = [ 
            .10, 
            .20, 
            .25, 
            .30, 
            .40, 
            .50, 
            .60, 
            .70, 
            .75, 
            .80, 
            .90, 
            .95, 
            .99 
        ]
        
        if len(data) == 0:
            data = [0]

        quantiles = {
            f'quantile_{int(quantile_range * 100)}th': quantile for quantile, quantile_range in zip(
                numpy.quantile(data, quantile_ranges),
                quantile_ranges
            )
        }
        
        return {
            group_name: {
                'median': float((numpy.median(data))),
                'mean': float(numpy.mean(data)),
                'variance': float(numpy.var(data)),
                'stdev': float(numpy.std(data)),
                'minimum': min(data),
                'maximum': max(data),
                'quantiles': quantiles
            }

        }