import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import json
from typing import Any, List

import psutil
from .json_config import JSONConfig
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup
has_connector = True

class JSON:

    def __init__(self, config: JSONConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        pass

    async def submit_events(self, events: List[BaseEvent]):
        event_records = [
            event.record for event in events
        ]

        with open(self.events_filepath, 'w') as events_file:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    json.dump,
                    event_records, 
                    events_file, 
                    indent=4
                )
            )

    async def submit_common(self, metrics_groups: List[MetricsGroup]):
        pass

    async def submit_metrics(self, metrics: List[MetricsGroup]):
        metrics_records = [
            {
                'name': metrics_group.name,
                'stage': metrics_group.stage,
                'errors': metrics_group.errors,
                **metrics_group.common_stats,
                'timings': {
                    timings_group_name: timings_group.record for timings_group_name, timings_group in metrics_group.groups.items()
                }
            } for metrics_group in metrics
        ]

        with open(self.metrics_filepath, 'w') as metrics_file:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    json.dump,
                    metrics_records, 
                    metrics_file, 
                    indent=4
                )
            )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):
        pass

    async def close(self):
        pass