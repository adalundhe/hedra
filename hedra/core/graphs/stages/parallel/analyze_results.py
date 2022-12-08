

import asyncio
import time
import traceback
import dill
import psutil
from collections import defaultdict
from typing import Any, Dict, List
from hedra.reporting.events import EventsGroup
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.hooks.registry.registrar import registrar


async def process_batch(
        stage_name: str, 
        custom_metric_hook_names: List[str],
        results_batch: List[BaseResult],
    ):

        events =  defaultdict(EventsGroup)

        custom_metric_hooks = []
        for metric_hook_name in custom_metric_hook_names:
            custom_metric_hook = registrar.all.get(metric_hook_name)
            custom_metric_hooks.append(custom_metric_hook)

        await asyncio.gather(*[
            asyncio.create_task(events[stage_result.name].add(
                    stage_result,
                    stage_name
                )) for stage_result in results_batch
        ])
        
        for events_group in events.values():  
            events_group.calculate_partial_group_stats()

        elapsed = 0
        start = time.monotonic()

        while elapsed < 15:
            await asyncio.sleep(0)
            elapsed = time.monotonic() - start

        return dill.dumps(events)


def group_batched_results(config: Dict[str, Any]):
    stage_name = config.get('analyze_stage_name')
    custom_metric_hook_names = config.get('analyze_stage_metric_hooks', [])
    results_batch = config.get('analyze_stage_batched_results', [])
    custom_event_types = config.get('analyze_stage_custom_event_types', [])

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(
            process_batch(
                stage_name,
                custom_metric_hook_names,
                results_batch
            )
        )

    except Exception as e:
        print(traceback.format_exc())
        raise e


# async def analyze_results_batched(
#         stage_name: str, 
#         stage_results: Dict[str, Union[int, List[BaseResult]]],
#         custom_metric_hook_names: List[str]=[],
#         assigned_workers_count: int = psutil.cpu_count(logical=False)
#     ):


#         custom_metric_hooks = []
#         for metric_hook_name in custom_metric_hook_names:
#             custom_metric_hook = registrar.all.get(metric_hook_name)
#             custom_metric_hooks.append(custom_metric_hook)

#         loop = asyncio.get_event_loop()
#         stage_total = 0
#         results = stage_results.get('results')
#         stage_total_time = stage_results.get('total_elapsed')
#         results_count = len(results)
        
#         batch_size = int(results_count/assigned_workers_count)

#         batched_results: List[List[BaseResult]] = []
        
#         for worker_idx in range(assigned_workers_count):

#             batch_marker = worker_idx * batch_size

#             batched_results.append(
#                 results[batch_marker:batch_marker + batch_size]
#             )

#         if results_count%assigned_workers_count > 0:
#             batched_results[assigned_workers_count-1].extend(
#                 results[assigned_workers_count * batch_size:]
#             )

#         executor = ProcessPoolExecutor(max_workers=assigned_workers_count)

#         groups = await asyncio.gather(*[
#             loop.run_in_executor(
#                 executor,
#                 group_batched_results,
#                 stage_name,
#                 custom_metric_hook_names,
#                 batch
#             ) for batch in batched_results
#         ])

#         batch_results: List[Dict[str, Union[dict, EventsGroup]]] = [
#             dill.loads(group) for group in groups
#         ]

#         events =  defaultdict(EventsGroup)

#         for events_groups in batch_results:
#             for event_group_name, events_group in events_groups.items():
#                 events[event_group_name].merge(events_group)

#         grouped_stats = {}
        
#         for event_group_name, events_group in events.items():  

#             custom_metrics = defaultdict(dict)
#             for custom_metric in custom_metric_hooks:
#                 custom_metrics[custom_metric.group][custom_metric.shortname] = await custom_metric.call(
#                     stage_results.get('results')
#                 )

#             events_group.calculate_quantiles()

#             metric_data = {
#                 'total': events_group.total,
#                 'succeeded': events_group.succeeded,
#                 'failed': events_group.failed,
#                 'actions_per_second': events_group.total/stage_total_time,
#                 'errors': list([
#                     {
#                         'message': error_message,
#                         'count': error_count
#                     } for error_message, error_count in events_group.errors.items()
#                 ]),
#                 'groups': events_group.groups,
#                 'custom': custom_metrics
#             }

#             metric = MetricsSet(
#                 event_group_name,
#                 events_group.source,
#                 stage_name,
#                 metric_data,
#                 events_group.tags
#             )

#             grouped_stats[event_group_name] = metric

#             stage_total += events_group.total

#         return {
#             'stage_metrics': {
#                 stage_name: {
#                     'total': stage_total,
#                     'actions_per_second': stage_total/stage_total_time,
#                     'actions': grouped_stats
#                 }
#             },
#             'stage_total': stage_total
#         }

