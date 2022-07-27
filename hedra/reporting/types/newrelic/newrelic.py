import asyncio
import functools
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


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
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        await self._loop.run_in_executor(
            None,
            functools.partial(
                newrelic.agent.initialize,
                config_file=self.config_path,
                environment=self.environment
            )
        )

        self.client = await self._loop.run_in_executor(
            None,
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
                None,
                functools.partial(
                    self.client.record_custom_event,
                    event.name,
                    event.record
                )
            )

    async def submit_metrics(self, metrics: List[Metric]):

        for metric in metrics:
            
            for field, value in metric.stats.items():
                await self._loop.run_in_executor(
                    None,
                    functools.partial(
                        self.client.record_custom_metric,
                        f'{metric.name}_{field}',
                        value
                    )
                )

    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.client.shutdown
        )