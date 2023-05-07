import asyncio
import csv
import psutil
import uuid
import os
import functools
import signal
import time
from typing import List, TextIO, Dict, Union
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric.metrics_set import MetricsSet
from .csv_config import CSVConfig
has_connector = True


def handle_loop_stop(
    signame, 
    executor: ThreadPoolExecutor, 
    loop: asyncio.AbstractEventLoop, 
    events_file: TextIO
): 
    try:
        events_file.close()
        executor.shutdown() 
        loop.stop()
    except Exception:
        pass
    

class CSV:

    def __init__(self, config: CSVConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = Path(config.metrics_filepath).absolute()
        self.experiments_filepath = config.experiments_filepath
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        
        experiments_path = Path(self.experiments_filepath)
        experiments_directory = experiments_path.parent
        experiments_filename = experiments_path.stem

        self.variants_filepath = os.path.join(
            experiments_directory,
            f'{experiments_filename}_variants.csv'
        )

        self.mutations_filepath = os.path.join(
            experiments_directory,
            f'{experiments_filename}_mutations.csv'
        )

        self.streams_filepath = config.streams_filepath

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._events_csv_writer: csv.DictWriter = None
        self._metrics_csv_writer: csv.DictWriter = None
        self._stage_metrics_csv_writer: csv.DictWriter = None
        self._errors_csv_writer: csv.DictWriter = None
        self._custom_metrics_csv_writer: csv.DictWriter = None
        self._experiments_writer: csv.DictWriter = None
        self._variants_writer: csv.DictWriter = None
        self._mutations_writer: csv.DictWriter = None
        self._streams_writer: csv.DictWriter = None

        self._loop: asyncio.AbstractEventLoop = None

        self.events_file: TextIO = None 
        self.metrics_file: TextIO = None
        self.experiments_file: TextIO = None
        self.variants_file: TextIO = None
        self.mutations_file: TextIO = None
        self.streams_file: TextIO = None

        self.write_mode = 'w' if config.overwrite else 'a'


        filepath = Path(config.metrics_filepath)
        base_filepath = filepath.parent
        base_filename = filepath.stem

        self.shared_metrics_filepath = os.path.join(
            base_filepath,
            f'{base_filename}_shared.csv'
        )

        self.custom_metrics_filepath = os.path.join(
            base_filepath,
            f'{base_filename}_custom.csv'
        )

        self.errors_metrics_filepath = os.path.join(
            base_filepath,
            f'{base_filename}_errors.csv'
        )

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')
        
        original_filepath = Path(self.events_filepath)
        
        directory = original_filepath.parent
        filename = original_filepath.stem

        events_file_timestamp = time.time()

        self.events_filepath = os.path.join(
            directory,
            f'{filename}_{events_file_timestamp}.csv'
        )

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):
        if self.streams_file is None:
            self.streams_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.streams_filepath,
                    self.write_mode
                )
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        self.streams_file
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Streams to file - {self.streams_filepath}')

        streams_data: List[Dict[str, Union[float, int]]] = []

        for stream_name, stream in stream_metrics.items():

            streams_data.extend([{
                    'stage': stream_name,
                    'group': group_name,
                    **group_metrics
            } for group_name, group_metrics in stream.grouped.items()])
            

        headers = list(streams_data[0].keys())

        if self._experiments_writer is None or self.write_mode == 'w':
            self._streams_writer = csv.DictWriter(
                self.streams_file, 
                fieldnames=headers
            )

            await self._loop.run_in_executor(
                self._executor,
                self._streams_writer.writeheader
            )

        await self._loop.run_in_executor(
            self._executor,
            self._streams_writer.writerows,
            streams_data
        )

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Experiments to file - {self.experiments_filepath}')

        if self.experiments_file is None:
            self.experiments_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.experiments_filepath,
                    self.write_mode
                )
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        self.experiments_file
                    )
                )

        if self._experiments_writer is None or self.write_mode == 'w':
            self._experiments_writer = csv.DictWriter(
                self.experiments_file, 
                fieldnames=experiment_metrics.experiments_metrics_fields
            )

            await self._loop.run_in_executor(
                self._executor,
                self._experiments_writer.writeheader
            )

        await self._loop.run_in_executor(
            self._executor,
            self._experiments_writer.writerows,
            experiment_metrics.experiments
        )

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        if self.variants_file is None:
            self.variants_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.variants_filepath,
                    self.write_mode
                )
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        self.variants_file
                    )
                )
        
        if self._variants_writer is None or self.write_mode == 'w':
            self._variants_writer = csv.DictWriter(
                self.variants_file, 
                fieldnames=experiment_metrics.variants_metrics_fields
            )

            await self._loop.run_in_executor(
                self._executor,
                self._variants_writer.writeheader
            )

        await self._loop.run_in_executor(
            self._executor,
            self._variants_writer.writerows,
            experiment_metrics.variants
        )

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        if self.mutations_file is None:
            self.mutations_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.mutations_filepath,
                    self.write_mode
                )
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        self.mutations_file
                    )
                )  

        if self._mutations_writer is None or self.write_mode == 'w':
            self._mutations_writer = csv.DictWriter(
                self.mutations_file, 
                fieldnames=experiment_metrics.mutations_metrics_fields
            )

            await self._loop.run_in_executor(
                self._executor,
                self._mutations_writer.writeheader
            )

        await self._loop.run_in_executor(
            self._executor,
            self._mutations_writer.writerows,
            experiment_metrics.mutations
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Experiments to file - {self.experiments_filepath}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Events to file - {self.events_filepath}')

        if self.events_file is None:
            self.events_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.events_filepath,
                    self.write_mode
                )
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        self.events_file
                    )
                )
          
        for event in events:
            if self._events_csv_writer is None or self.write_mode == 'w':
                self._events_csv_writer = csv.DictWriter(self.events_file, fieldnames=event.fields)
                
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

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop,
                    shared_metrics_file
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

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop,
                    metrics_file
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

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        custom_metrics_file
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

        errors_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                self.errors_metrics_filepath,
                'w'
            )
        )

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop,
                    errors_file
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
        
        if self.events_file:
            await self._loop.run_in_executor(
                self._executor,
                self.events_file.close
            )

        if self.experiments_file:
            await self._loop.run_in_executor(
                self._executor,
                self.experiments_file.close
            )

        if self.variants_file:
            await self._loop.run_in_executor(
                self._executor,
                self.variants_file.close
            )

        if self.mutations_file:
            await self._loop.run_in_executor(
                self._executor,
                self.mutations_file.close
            )

        if self.streams_file:
            await self._loop.run_in_executor(
                self._executor,
                self.streams_file.close
            )

        self._executor.shutdown()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')

