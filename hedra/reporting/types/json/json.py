import asyncio
import functools
import json
import psutil
import uuid
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet
from .json_config import JSONConfig

has_connector = True

class JSON:

    def __init__(self, config: JSONConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()


    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Events to file - {self.events_filepath}')

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Events to file - {self.events_filepath}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping Shared Metrics')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Metrics to file - {self.metrics_filepath}')

        records = {}
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            groups = metrics_set.custom_metrics

            for group_name, group in metrics_set.groups.items():
                groups[group_name] = group.record

            records[metrics_set.name] = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'errors': metrics_set.errors,
                **metrics_set.common_stats,
                'groups': groups
            }

        with open(self.metrics_filepath, 'w') as metrics_file:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    json.dump,
                    records, 
                    metrics_file, 
                    indent=4
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Metrics to file - {self.metrics_filepath}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping Custom Metrics')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping Error Metrics')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')