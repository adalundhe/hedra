import asyncio
import functools
import psutil
import uuid
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from .honeycomb_config import HoneycombConfig


try:
    import libhoney
    has_connector = True

except Exception:
    libhoney = None
    has_connector = False


class Honeycomb:

    def __init__(self, config: HoneycombConfig) -> None:
        self.api_key = config.api_key
        self.dataset = config.dataset
        self.client = None
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=True))
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()


    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Honeycomb.IO')
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                libhoney.init,
                writekey=self.api_key,
                dataset=self.dataset
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Honeycomb.IO')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Honeycomb.IO')

        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stream - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped.items():

                group_metric = {
                    'name': f'{stage_name}_streams',
                    'stage': stage_name,
                    'group': group_name,
                    **group
                }

                honeycomb_group_metric = libhoney.Event(data=group_metric)

                await self._loop.run_in_executor(
                    self._executor,
                    honeycomb_group_metric.send
                )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Honeycomb.IO')

    async def submit_experiments(self, expoeriment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Honeycomb.IO')

        for experiment in expoeriment_metrics.experiment_summaries:

            honeycomb_event = libhoney.Event(data={
                **experiment.record
            })

            await self._loop.run_in_executor(
                self._executor,
                honeycomb_event.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Honeycomb.IO')

    async def submit_variants(self, expoeriment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Honeycomb.IO')

        for variant in expoeriment_metrics.variant_summaries:

            honeycomb_event = libhoney.Event(data={
                **variant.record
            })

            await self._loop.run_in_executor(
                self._executor,
                honeycomb_event.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Honeycomb.IO')
    
    async def submit_mutations(self, expoeriment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Honeycomb.IO')

        for mutation in expoeriment_metrics.mutation_summaries:

            honeycomb_event = libhoney.Event(data={
                **mutation.record
            })

            await self._loop.run_in_executor(
                self._executor,
                honeycomb_event.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Honeycomb.IO')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Honeycomb.IO')

        for event in events:

            honeycomb_event = libhoney.Event(data={
                **event.record
            })

            await self._loop.run_in_executor(
                self._executor,
                honeycomb_event.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Honeycomb.IO')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Honeycomb.IO')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            group_metric = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                **metrics_set.common_stats
            }

            honeycomb_group_metric = libhoney.Event(data=group_metric)

            await self._loop.run_in_executor(
                self._executor,
                honeycomb_group_metric.send
            )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Honeycomb.IO')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Honeycomb.IO')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():

                metric_record = {
                    **group.stats, 
                    **group.custom,
                    'group': group_name
                }     
    
                honeycomb_event = libhoney.Event(data=metric_record)

                await self._loop.run_in_executor(
                    self._executor,
                    honeycomb_event.send
                )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Honeycomb.IO')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Honeycomb.IO')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custm_metric_name, custom_metric in metrics_set.custom_metrics.items():

                metric_record ={
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'custom',
                    custm_metric_name: custom_metric.metric_value
                }

                honeycomb_event = libhoney.Event(data=metric_record)

                await self._loop.run_in_executor(
                    self._executor,
                    honeycomb_event.send
                )

        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Honeycomb.IO')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Honeycomb.IO')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:

                error_event = libhoney.Event(data={
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                })

                await self._loop.run_in_executor(
                    self._executor,
                    error_event.send
                )


        await self._loop.run_in_executor(
            self._executor,
            libhoney.flush
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Honeycomb.IO')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection Honeycomb.IO')

        await self._loop.run_in_executor(
            self._executor,
            libhoney.close
        )

        self._executor.shutdown()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection Honeycomb.IO')

