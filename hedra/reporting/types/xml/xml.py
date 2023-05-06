import asyncio
import functools
import collections
import collections.abc
import uuid
import psutil
import signal
import os
import time
from pathlib import Path
from typing import List, TextIO, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from xml.dom.minidom import parseString
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric.metrics_set import MetricsSet
from .xml_config import XMLConfig

try:
    from dicttoxml import dicttoxml
except Exception:
    dicttoxml = object

collections.Iterable = collections.abc.Iterable


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
    

class XML:
    
    def __init__(self, config: XMLConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = Path(config.metrics_filepath).absolute()
        self.experiments_filepath = config.experiments_filepath
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop: asyncio.AbstractEventLoop = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        experiments_path = Path(self.experiments_filepath)
        experiments_directory = experiments_path.parent
        experiments_filename = experiments_path.stem

        self.variants_filepath = os.path.join(
            experiments_directory,
            f'{experiments_filename}_variants.xml'
        )

        self.mutations_filepath = os.path.join(
            experiments_directory,
            f'{experiments_filename}_mutations.xml'
        )

        filepath = Path(config.metrics_filepath)
        base_filepath = filepath.parent
        base_filename = filepath.stem

        self.shared_metrics_filepath = os.path.join(
            base_filepath,
            f'{base_filename}_shared.xml'
        )

        self.custom_metrics_filepath = os.path.join(
            base_filepath,
            f'{base_filename}_custom.xml'
        )

        self.errors_metrics_filepath = os.path.join(
            base_filepath,
            f'{base_filename}_errors.xml'
        )

        self.streams_metrics_filepath = config.streams_filepath

        self.events_file: TextIO = None
        self.metrics_file: TextIO = None
        self.experiments_file: TextIO = None
        self.variants_file: TextIO = None
        self.mutations_file: TextIO = None
        self.streams_file: TextIO = None

        self.write_mode = 'w' if config.overwrite else 'a'

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):
        if self.streams_file is None:
            self.streams_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.streams_metrics_filepath,
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Streams to file - {self.streams_metrics_filepath}')

        streams_data = [
            {
                'stage': stream_name,
                **stream_set.grouped 
            } for stream_name, stream_set in stream_metrics.items()
        ]

        streams_xml = dicttoxml(
            streams_data, 
            custom_root='streams'
        )

        streams_xml = parseString(streams_xml)

        await self._loop.run_in_executor(
            self._executor,
            self.streams_file.write,
            streams_xml.toprettyxml()
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

        experiments_xml = dicttoxml(
            experiment_metrics.experiments, 
            custom_root='experiments'
        )

        experiments_xml = parseString(experiments_xml)

        await self._loop.run_in_executor(
            self._executor,
            self.experiments_file.write,
            experiments_xml.toprettyxml()
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

        variants_xml = dicttoxml(
            experiment_metrics.variants, 
            custom_root='variants'
        )

        variants_xml = parseString(variants_xml)

        await self._loop.run_in_executor(
            self._executor,
            self.variants_file.write,
            variants_xml.toprettyxml()
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

        

        mutations_xml = dicttoxml(
            experiment_metrics.mutations, 
            custom_root='mutations'
        )
        
        mutations_xml = parseString(mutations_xml)

        await self._loop.run_in_executor(
            self._executor,
            self.mutations_file.write,
            mutations_xml.toprettyxml()
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Events to file - {self.events_filepath}')

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')
        
        original_filepath = Path(self.events_filepath)
        
        directory = original_filepath.parent
        filename = original_filepath.stem

        events_file_timestamp = time.time()

        self.events_filepath = os.path.join(
            directory,
            f'{filename}_{events_file_timestamp}.xml'
        )

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

        events_xml = dicttoxml([
            event.to_dict() for event in events
        ], custom_root='events')

        events_xml = parseString(events_xml)

        await self._loop.run_in_executor(
            self._executor,
            self.events_file.write,
            events_xml.toprettyxml()
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Events to file - {self.events_filepath}')

    async def submit_common(self, metrics: List[MetricsSet]):
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Shared Metrics to file - {self.metrics_filepath}')

        shared_metrics_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.shared_metrics_filepath,
                    'w'
                )
        )

        common_metrics_xml = dicttoxml([
            {
                'name': metric_set.name,
                'stage': metric_set.stage,
                'group': 'common',
                **metric_set.common_stats
            } for metric_set in metrics
        ], custom_root='common_metrics')

        common_metrics_xml = parseString(common_metrics_xml)

        await self._loop.run_in_executor(
            self._executor,
            shared_metrics_file.write,
            common_metrics_xml.toprettyxml()
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

        metrics_data = []
        for metrics_set in metrics:
            for group_name, group in metrics_set.groups.items():
                metrics_data.append({
                    **group.record,
                    'group': group_name
                })


        metrics_xml = dicttoxml(metrics_data, custom_root='metrics')
        metrics_xml = parseString(metrics_xml)

        await self._loop.run_in_executor(
            self._executor,
            metrics_file.write,
            metrics_xml.toprettyxml()
        )

        await self._loop.run_in_executor(
            self._executor,
            metrics_file.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Metrics to file - {self.metrics_filepath}')

    async def submit_custom(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Custom Metrics to file - {self.metrics_filepath}')

        custom_metrics_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                self.custom_metrics_filepath,
                'w'
            )       
        )

        metrics_data = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Group - Custom')

            metrics_data.append({
                **{
                    cusom_metric_name: custom_metric.metric_value for cusom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                },
                'group': 'custom'
            })

        custom_metrics_xml = dicttoxml(metrics_data, custom_root='custom_metrics')
        custom_metrics_xml = parseString(custom_metrics_xml)

        await self._loop.run_in_executor(
            self._executor,
            custom_metrics_file.write,
            custom_metrics_xml.toprettyxml()
        )

        await self._loop.run_in_executor(
            self._executor,
            custom_metrics_file.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Custom Metrics to file - {self.metrics_filepath}')


    async def submit_errors(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Error Metrics to file - {self.metrics_filepath}')
        
        errors_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                self.errors_metrics_filepath,
                'w'
            )
        )

        errors = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                errors.append({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                })

        errors_xml = dicttoxml(errors, custom_root='errors')
        errors_xml = parseString(errors_xml)

        await self._loop.run_in_executor(
            self._executor,
            errors_file.write,
            errors_xml.toprettyxml()
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
