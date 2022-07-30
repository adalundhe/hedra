import asyncio
import csv
import psutil
from typing import List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric
from hedra.reporting.metric.metrics_group import MetricsGroup
from .csv_config import CSVConfig
has_connector = True


class CSV:

    def __init__(self, config: CSVConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

        self._events_csv_writer = None
        self._metrics_csv_writer = None
        self._errors_csv_writer = None

    async def connect(self):
        pass

    async def submit_events(self, events: List[BaseEvent]):


        with open(self.events_filepath, 'w') as events_file:

            for event in events:
                if self._events_csv_writer is None:
                    self._events_csv_writer = csv.DictWriter(events_file, fieldnames=event.fields)

                    await self._loop.run_in_executor(
                        self._executor,
                        self._events_csv_writer.writeheader
                    )

                await self._loop.run_in_executor(
                    self._executor,
                    self._events_csv_writer.writerow,
                    event.record
                )

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        with open(self.metrics_filepath, 'w') as metrics_file:

            for metrics_group in metrics:
                if self._metrics_csv_writer is None:
                    self._metrics_csv_writer = csv.DictWriter(metrics_file, fieldnames=[
                        *metrics_group.fields,
                        'timings_group'
                    ])

                    await self._loop.run_in_executor(
                        self._executor,
                        self._metrics_csv_writer.writeheader
                    )

                for timings_group_name, timings_group in metrics_group.groups.items():
                    await self._loop.run_in_executor(
                        self._executor,
                        self._metrics_csv_writer.writerow,
                        {
                            **timings_group.record,
                            'timings_group': timings_group_name
                        }
                    )

    async def submit_events(self, metrics_groups: List[MetricsGroup]):

        error_csv_headers = [
            'metric_name',
            'metric_stage',
            'error_message',
            'error_count'
        ]

        errors_file_path = Path(self.metrics_filepath).stem
        with open (f'{errors_file_path}_errors.csv', 'w') as errors_file:
            
            error_csv_writer = csv.DictWriter(errors_file, fieldnames=error_csv_headers)

            await self._loop.run_in_executor(
                self._executor,
                error_csv_writer.writeheader
            )

            for metrics_group in metrics_groups:

                for error in metrics_group.errors:
                    await self._loop.run_in_executor(
                        self._executor,
                        error_csv_writer.writerow,
                        {
                            'metric_name': metrics_group.name,
                            'metric_stage': metrics_group.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        }
                    )
                

    async def close(self):
        pass

