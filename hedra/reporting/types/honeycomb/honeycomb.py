import asyncio
import functools
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    import libhoney
    from .honeycomb_config import HoneycombConfig
    has_connector = True

except ImportError:
    libhoney = None
    HoneycombConfig = None
    has_connector = False


class Honeycomb:

    def __init__(self, config: HoneycombConfig) -> None:
        self.api_key = config.api_key
        self.dataset = config.dataset
        self.client = None
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
                None,
                Honeycomb_event.send
            )

        await self._loop.run_in_executor(
            None,
            libhoney.flush
        )

    async def submit_metrics(self, metrics: List[Metric]):

        for metric in metrics:
            
            named_metric = {
                f'{metric.name}_{field}': value for field, value in metric.stats.items()
            }
            honeycomb_event = libhoney.Event(data={
                **named_metric
            })

            await self._loop.run_in_executor(
                None,
                honeycomb_event.send
            )

        await self._loop.run_in_executor(
            None,
            libhoney.flush
        )

    async def close(self):
        await self._loop.run_in_executor(
            None,
            libhoney.close
        )

