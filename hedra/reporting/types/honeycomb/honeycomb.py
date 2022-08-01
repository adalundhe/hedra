import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import re
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


try:
    import libhoney
    from .honeycomb_config import HoneycombConfig
    has_connector = True

except Exception:
    libhoney = None
    HoneycombConfig = None
    has_connector = False


class Honeycomb:

    def __init__(self, config: HoneycombConfig) -> None:
        self.api_key = config.api_key
        self.dataset = config.dataset
        self.client = None
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=True))
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                libhoney.init,
                writekey=self.api_key,
                dataset=self.dataset
            )
        )

    async def submit_events(self, events: List[BaseEvent]):

        for event in events:

            Honeycomb_event = libhoney.Event(data={
                **event.record
            })

            await self._loop.run_in_executor(
                self._executor,
                Honeycomb_event.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

    async def submit_common(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:

            group_metric = {
                'name': metrics_group.name,
                'stage': metrics_group.stage,
                **metrics_group.common_stats
            }

            honeycomb_group_metric = libhoney.Event(data=group_metric)

            await self._loop.run_in_executor(
                self._executor,
                honeycomb_group_metric.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:

            for group_name, group in metrics_group.groups.items():

                metric_record = {
                    **group.stats, 
                    **group.custom,
                    'group': group_name
                }     
    
                honeycomb_event = libhoney.Event(data=metric_record)

                await self._loop.run_in_executor(
                    self._executor,
                    honeycomb_event.send
                )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:
            for error in metrics_group.errors:

                error_event = libhoney.Event(data={
                    'name': metrics_group.name,
                    'stage': metrics_group.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                })

                await self._loop.run_in_executor(
                    self._executor,
                    error_event.send
                )


        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

    async def close(self):
        await self._loop.run_in_executor(
            self._executor,
            libhoney.close
        )

