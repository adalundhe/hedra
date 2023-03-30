import asyncio
import functools
import collections
import collections.abc
import uuid
import psutil
from pathlib import Path
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from dicttoxml import dicttoxml
from xml.dom.minidom import parseString
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.metrics_set import MetricsSet
from .xml_config import XMLConfig


collections.Iterable = collections.abc.Iterable


class XML:
    
    def __init__(self, config: XMLConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = Path(config.metrics_filepath).absolute()
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        filepath = Path(config.metrics_filepath)
        base_filepath = filepath.parent
        base_filename = filepath.stem

        self.shared_metrics_filepath = f'{base_filepath}/{base_filename}_shared.xml'
        self.custom_metrics_filepath = f'{base_filepath}/{base_filename}_custom.xml'
        self.errors_metrics_filepath = f'{base_filepath}/{base_filename}_errors.xml'

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping connect')

    async def submit_events(self, events: List[BaseProcessedResult]):

        events_file = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                open,
                self.events_filepath,
                'w'
            )
        )   

        events_xml = dicttoxml([
            event.to_dict() for event in events
        ], custom_root='events')

        events_xml = parseString(events_xml)

        await self._loop.run_in_executor(
            self._executor,
            events_file.write,
            events_xml.toprettyxml()
        )

        await self._loop.run_in_executor(
            self._executor,
            events_file.close
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
        self._executor.shutdown()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
