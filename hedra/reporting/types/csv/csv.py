import asyncio
import csv
import psutil
import uuid
from typing import List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
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

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._events_csv_writer = None
        self._metrics_csv_writer = None
        self._stage_metrics_csv_writer = None
        self._errors_csv_writer = None
        self._custom_metrics_csv_writers = {}

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Events to file - {self.events_filepath}')
        with open(self.events_filepath, 'a') as events_file:

            for event in events:
                if self._events_csv_writer is None:
                    self._events_csv_writer = csv.DictWriter(events_file, fieldnames=event.fields)
                    
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(event.fields)}')

                    await self._loop.run_in_executor(
                        self._executor,
                        self._events_csv_writer.writeheader
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(event.fields)}')

                await self._loop.run_in_executor(
                    self._executor,
                    self._events_csv_writer.writerow,
                    event.record
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Events to file - {self.events_filepath}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Shared Metrics to file - {self.metrics_filepath}')

        headers = [
            'name',
            'stage',
            'group',
            'total',
            'succeeded',
            'failed'
        ]

        base_filepath = Path(self.metrics_filepath).parent
        with open(f'{base_filepath}/stage_metrics.csv', 'a') as shared_metrics_file:

            if self._stage_metrics_csv_writer is None:
                self._stage_metrics_csv_writer = csv.DictWriter(shared_metrics_file, fieldnames=headers)


                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}')

                await self._loop.run_in_executor(
                    self._executor,
                    self._stage_metrics_csv_writer.writeheader
                )

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                await self._loop.run_in_executor(
                    self._executor,
                    self._stage_metrics_csv_writer.writerow,
                    {
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    }
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Shared Metrics to file - {self.metrics_filepath}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Metrics to file - {self.metrics_filepath}')

        with open(self.metrics_filepath, 'a') as metrics_file:

            for metrics_set in metrics:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._metrics_csv_writer is None:

                    headers = [
                        *metrics_set.fields,
                        'group'
                    ]

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}')

                    self._metrics_csv_writer = csv.DictWriter(metrics_file, fieldnames=headers)

                    await self._loop.run_in_executor(
                        self._executor,
                        self._metrics_csv_writer.writeheader
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}')

                for group_name, group in metrics_set.groups.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}')

                    await self._loop.run_in_executor(
                        self._executor,
                        self._metrics_csv_writer.writerow,
                        {
                            **group.record,
                            'group': group_name
                        }
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Metrics to file - {self.metrics_filepath}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Custom Metrics to file - {self.metrics_filepath}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
                
            for custom_group_name, group in metrics_set.custom_metrics.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Group - {custom_group_name}')

                base_filepath = Path(self.metrics_filepath).parent
                with open(f'{base_filepath}/{custom_group_name}.csv', 'a') as custom_metrics_file:

                    headers = [
                        'name',
                        'stage',
                        'group',
                        *list(group.keys())
                    ]
                    
                    if self._custom_metrics_csv_writers.get(custom_group_name) is None:

                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}')

                        custom_group_csv_writer = csv.DictWriter(custom_metrics_file, fieldnames=headers)
                        self._custom_metrics_csv_writers[custom_group_name] = custom_group_csv_writer

                        await self._loop.run_in_executor(
                            self._executor,
                            custom_group_csv_writer.writeheader
                        )

                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}')


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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Custom Metrics to file - {self.metrics_filepath}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Error Metrics to file - {self.metrics_filepath}')

        error_csv_headers = [
            'name',
            'stage',
            'error_message',
            'error_count'
        ]

        base_filepath = Path(self.metrics_filepath).parent
        with open (f'{base_filepath}/stage_errors.csv', 'a') as errors_file:
            
            if self._errors_csv_writer is None:

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(error_csv_headers)}')
                error_csv_writer = csv.DictWriter(errors_file, fieldnames=error_csv_headers)

                await self._loop.run_in_executor(
                    self._executor,
                    error_csv_writer.writeheader
                )

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(error_csv_headers)}')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Error Metrics to file - {self.metrics_filepath}')
                
    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')

