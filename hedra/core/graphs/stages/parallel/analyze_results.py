

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
        raise e
