import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import re
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup, timings_group


try:
    import newrelic.agent
    from .newrelic_config import NewRelicConfig
    has_connector=True

except ImportError:
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

    async def connect(self):
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

    async def submit_events(self, events: List[BaseEvent]):
        for event in events:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.record_custom_event,
                    event.name,
                    event.record
                )
            )

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:

            for timings_group_name, timings_group in metrics_group.groups.items():

                metric_record = {
                    **timings_group.stats, 
                    **timings_group.custom,
                    'timings_group': timings_group_name
                }
                
                for field, value in metric_record.items():
                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.client.record_custom_metric,
                            f'{metrics_group.name}_{field}',
                            value
                        )
                    )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):
        for metrics_group in metrics_groups:
            for error in metrics_group.errors:
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
                        f'{metrics_group.name}_{error_message}',
                        error.get('count')
                    )
                )            

    async def close(self):
        await self._loop.run_in_executor(
            self._executor,
            self.client.shutdown
        )