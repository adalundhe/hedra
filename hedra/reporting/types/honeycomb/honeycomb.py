import asyncio
import functools
import psutil
import uuid
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


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

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()


    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Honeycomb.IO')
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                libhoney.init,
                writekey=self.api_key,
                dataset=self.dataset
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Honeycomb.IO')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Honeycomb.IO')

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Honeycomb.IO')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Honeycomb.IO')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            group_metric = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                **metrics_set.common_stats
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


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Honeycomb.IO')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Honeycomb.IO')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Honeycomb.IO')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Honeycomb.IO')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():

                metric_record ={
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Honeycomb.IO')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Honeycomb.IO')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:

                error_event = libhoney.Event(data={
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Honeycomb.IO')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection Honeycomb.IO')

        await self._loop.run_in_executor(
            self._executor,
            libhoney.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection Honeycomb.IO')

