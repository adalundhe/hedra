import asyncio
import csv
import psutil
from typing import List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric.metrics_set import MetricsSet
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
        self._group_metrics_csv_writer = None
        self._errors_csv_writer = None
        self._custom_metrics_csv_writers = {}

    async def connect(self):
        pass

    async def submit_events(self, events: List[BaseEvent]):


        with open(self.events_filepath, 'a') as events_file:

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

    async def submt_common(self, metrics_sets: List[MetricsSet]):

        headers = [
            'name',
            'stage',
            'group',
            'total',
            'succeeded',
            'failed'
        ]

        group_metrics_filepath = Path(self.metrics_filepath).stem
        with open(f'{group_metrics_filepath}_group_metrics.csv', 'a') as group_metrics_file:

            if self._group_metrics_csv_writer is None:
                self._group_metrics_csv_writer = csv.DictWriter(group_metrics_filepath, fieldnames=headers)

                await self._loop.run_in_executor(
                    self._executor,
                    self._group_metrics_csv_writer.writeheader
                )

            for metrics_set in metrics_sets:
                await self._loop.run_in_executor(
                    self._executor,
                    self._group_metrics_csv_writer.writerow,
                    {
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    }
                )


    async def submit_metrics(self, metrics: List[MetricsSet]):

        with open(self.metrics_filepath, 'a') as metrics_file:

            for metrics_set in metrics:
                if self._metrics_csv_writer is None:
                    self._metrics_csv_writer = csv.DictWriter(metrics_file, fieldnames=[
                        *metrics_set.fields,
                        'group'
                    ])

                    await self._loop.run_in_executor(
                        self._executor,
                        self._metrics_csv_writer.writeheader
                    )

                for group_name, group in metrics_set.groups.items():
                    await self._loop.run_in_executor(
                        self._executor,
                        self._metrics_csv_writer.writerow,
                        {
                            **group.record,
                            'group': group_name
                        }
                    )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():

                base_filepath = Path(self.metrics_filepath).stem
                with open(f'{base_filepath}_{custom_group_name}.csv', 'a') as custom_metrics_file:

                    headers = [
                        'name',
                        'stage',
                        'group',
                        *list(group.keys())
                    ]
                    
                    if self._custom_metrics_csv_writers.get(custom_group_name) is None:
                        custom_group_csv_writer = csv.DictWriter(custom_metrics_file, fieldnames=headers)
                        self._custom_metrics_csv_writers[custom_group_name] = custom_group_csv_writer

                        await self._loop.run_in_executor(
                            self._executor,
                            custom_group_csv_writer.writeheader
                        )


                    await self._loop.run_in_executor(
                        self._executor,
                        self._custom_metrics_csv_writers[custom_group_name].writerow,
                        {
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        }
                    )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        error_csv_headers = [
            'name',
            'stage',
            'error_message',
            'error_count'
        ]

        errors_file_path = Path(self.metrics_filepath).stem
        with open (f'{errors_file_path}_errors.csv', 'a') as errors_file:
            
            if self._errors_csv_writer is None:
                error_csv_writer = csv.DictWriter(errors_file, fieldnames=error_csv_headers)

                await self._loop.run_in_executor(
                    self._executor,
                    error_csv_writer.writeheader
                )

            for metrics_set in metrics_sets:

                for error in metrics_set.errors:
                    await self._loop.run_in_executor(
                        self._executor,
                        error_csv_writer.writerow,
                        {
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        }
                    )
                

    async def close(self):
        pass

