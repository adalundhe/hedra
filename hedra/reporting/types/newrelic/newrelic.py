import asyncio
import functools
import re
import psutil
import uuid
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    import newrelic.agent
    from .newrelic_config import NewRelicConfig
    has_connector=True

except Exception:
    newrelic = None
    NewRelicConfig = None
    has_connector=False


class NewRelic:

    def __init__(self, config: NewRelicConfig) -> None:
        self.config_path = config.config_path
        self.environment = config.environment
        self.registration_timeout = config.registration_timeout
        self.shutdown_timeout = config.shutdown_timeout or 60
        self.newrelic_application_name = config.newrelic_application_name

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to NewRelic - Using config at path - {self.config_path} - Environment - {self.environment}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                newrelic.agent.initialize,
                config_file=self.config_path,
                environment=self.environment
            )
        )

        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                newrelic.agent.register_application,
                name=self.newrelic_application_name,
                timeout=self.registration_timeout
            )
        )

        await asyncio.sleep(1)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to NewRelic - Using config at path - {self.config_path} - Environment - {self.environment}')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to NewRelic')

        for event in events:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.record_custom_event,
                    event.name,
                    event.record
                )
            )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to NewRelic')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to NewRelic')
        
        for metrics_set in metrics_sets:  
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}')
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        f'{metrics_set.name}_{field}',
                        value
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to NewRelic')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to NewRelic')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                for field, value in group.stats.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{field}')
                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.client.record_custom_metric,
                            f'{metrics_set.name}_{group_name}_{field}',
                            value
                        )
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to NewRelic')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to NewRelic')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():
                for field, value in group.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metric - {metrics_set.name}:{custom_group_name}:{field}')
                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.client.record_custom_metric,
                            f'{metrics_set.name}_{custom_group_name}_{field}',
                            value
                        )
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to NewRelic')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to NewRelic')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                error_message = re.sub(
                    '[^0-9a-zA-Z]+', 
                    '_',
                    error.get(
                        'message'
                    ).lower()
                )

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        f'{metrics_set.name}_errors_{error_message}',
                        error.get('count')
                    )
                )  
                
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to NewRelic')          

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to NewRelic')

        await self._loop.run_in_executor(
            self._executor,
            self.client.shutdown
        )

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to NewRelic')