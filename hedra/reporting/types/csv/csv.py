import asyncio
import csv
import psutil
import uuid
import functools
from typing import List
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.metrics_set import MetricsSet
from .csv_config import CSVConfig
has_connector = True


class CSV:

    def __init__(self, config: CSVConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = Path(config.metrics_filepath).absolute()
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
        self._custom_metrics_csv_writer = None


        filepath = Path(config.metrics_filepath)
        base_filepath = filepath.parent
        base_filename = filepath.stem

        self.shared_metrics_filepath = f'{base_filepath}/{base_filename}_shared.csv'
        self.custom_metrics_filepath = f'{base_filepath}/{base_filename}_custom.csv'
        self.errors_metrics_filepath = f'{base_filepath}/{base_filename}_errors.csv'

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Events to file - {self.events_filepath}')
        
        events_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                self.events_filepath,
                'w'
            )
        )        

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

        await self._loop.run_in_executor(
            self._executor,
            events_file.close
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
            'failed',
            'actions_per_second'
        ]

        shared_metrics_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.shared_metrics_filepath,
                    'w'
                )
        )

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

        await self._loop.run_in_executor(
            self._executor,
            shared_metrics_file.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Shared Metrics to file - {self.metrics_filepath}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Metrics to file - {self.metrics_filepath}')

        metrics_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                self.metrics_filepath, 
                'w'
            )
        )

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

        await self._loop.run_in_executor(
            self._executor,
            metrics_file.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Metrics to file - {self.metrics_filepath}')
        
    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Custom Metrics to file - {self.metrics_filepath}')

        custom_metrics_file = None
        
        headers = [
            'name',
            'stage',
            'group',
        ]

        if self._custom_metrics_csv_writer is None:

            custom_metrics_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.custom_metrics_filepath,
                    'w'
                )       
            )

            for metrics_set in metrics_sets:
                for custom_metric in metrics_set.custom_metrics.values():
                    headers.append(
                        custom_metric.metric_name
                    )


            self._custom_metrics_csv_writer = csv.DictWriter(custom_metrics_file, fieldnames=headers)
            
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Writing headers to file - {self.metrics_filepath} - {", ".join(headers)}')

            await self._loop.run_in_executor(
                self._executor,
                self._custom_metrics_csv_writer.writeheader
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Wrote headers to file - {self.metrics_filepath} - {", ".join(headers)}')


        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Group - Custom')

            await self._loop.run_in_executor(
                self._executor,
                self._custom_metrics_csv_writer.writerow,
                {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'custom',
                    **{
                        custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    }
                }
            )

        await self._loop.run_in_executor(
            self._executor,
            custom_metrics_file.close
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

        filepath = Path(self.metrics_filepath)
        base_filepath = filepath.parent
        base_filename = filepath.stem

        errors_filepath = f'{base_filepath}/{base_filename}_errors,csv'

        errors_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                errors_filepath,
                'w'
            )
        )
            
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

        await self._loop.run_in_executor(
            self._executor,
            errors_file.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Error Metrics to file - {self.metrics_filepath}')
                
    async def close(self):
        self._executor.shutdown()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')

