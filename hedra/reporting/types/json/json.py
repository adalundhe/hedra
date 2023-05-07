from __future__ import annotations
import asyncio
import functools
import json
import psutil
import uuid
import os
import signal
import re
import time
from pathlib import Path
from typing import List, TextIO, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from .json_config import JSONConfig


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


class JSON:

    def __init__(self, config: JSONConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath
        self.experiments_filepath = config.experiments_filepath
        self.streams_filepath = config.streams_filepath
        
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop: asyncio.AbstractEventLoop = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.events_file: TextIO = None
        self.experiments_file: TextIO = None
        self.metrics_file: TextIO = None
        self.streams_file: TextIO = None

        self.write_mode = 'w' if config.overwrite else 'a'
        self.pattern = re.compile("_copy[0-9]+")

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')
        
        original_filepath = Path(self.events_filepath)
        
        directory = original_filepath.parent
        filename = original_filepath.stem

        events_file_timestamp =time.time()
        self.events_filepath = os.path.join(
            directory,
            f'{filename}_{events_file_timestamp}.json'
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

        streams_data ={
            stream_name: stream_set.grouped for stream_name, stream_set in stream_metrics.items()
        }

        if self.metrics_file is None:
            self.metrics_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.metrics_filepath,
                    self.write_mode
                )
            )

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                json.dump,
                streams_data, 
                self.streams_file, 
                indent=4
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Streams to file - {self.streams_filepath}')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Experiments to file - {self.experiments_filepath}')

        if self.experiments_file is None:
            self.experiments_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.experiments_filepath,
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
                        self.experiments_file
                    )
                )

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                json.dump,
                {
                    'experiments': experiment_metrics.experiments,
                    'variants': experiment_metrics.variants,
                    'mutations': experiment_metrics.mutations
                }, 
                self.experiments_file, 
                indent=4
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Experiments to file - {self.experiments_filepath}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):
        pass

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):
        pass

    async def submit_events(self, events: List[BaseProcessedResult]):

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Events to file - {self.events_filepath}')

        event_records = {
            event.event_id: event.record for event in events
        }

        if self.write_mode == 'a+':
            try:
                existing_data = await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        json.load,
                        self.events_file
                    )
                )
            
            except Exception:
                existing_data = {}

            event_records.update(existing_data)


        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                json.dump,
                event_records, 
                self.events_file, 
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
            
            groups = {}
            for group_name, group in metrics_set.groups.items():
                groups[group_name] = group.record

            groups['custom'] = {
                metric.metric_shortname: metric.metric_value for metric in metrics_set.custom_metrics.values()
            }

            records[metrics_set.name] = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'errors': metrics_set.errors,
                **metrics_set.common_stats,
                'groups': groups
            }

        if self.metrics_file is None:
            self.metrics_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.metrics_filepath,
                    self.write_mode
                )
            )

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                json.dump,
                records, 
                self.metrics_file, 
                indent=4
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Metrics to file - {self.metrics_filepath}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping Custom Metrics')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping Error Metrics')

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

        if self.metrics_file:
            await self._loop.run_in_executor(
                self._executor,
                self.metrics_file.close
            )  

        if self.streams_file:      
            await self._loop.run_in_executor(
                self._executor,
                self.streams_file.close
            )  
            
        self._executor.shutdown()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')