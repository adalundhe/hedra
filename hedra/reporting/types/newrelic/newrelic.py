import asyncio
import functools
import re
import psutil
import uuid
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.system.system_metrics_set import (
    SystemMetricsSet,
    SessionMetricsCollection,
    SystemMetricsCollection
)

try:
    import newrelic.agent
    from .newrelic_config import NewRelicConfig
    has_connector=True

except Exception:
    newrelic = None
    NewRelicConfig = None
    has_connector=False


class NewRelic:

    def __init__(self, config: NewRelicConfig) -> None:
        self.config_path = config.config_path
        self.environment = config.environment
        self.registration_timeout = config.registration_timeout
        self.shutdown_timeout = config.shutdown_timeout or 60
        self.newrelic_application_name = config.newrelic_application_name

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to NewRelic - Using config at path - {self.config_path} - Environment - {self.environment}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                newrelic.agent.initialize,
                config_file=self.config_path,
                environment=self.environment
            )
        )

        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                newrelic.agent.register_application,
                name=self.newrelic_application_name,
                timeout=self.registration_timeout
            )
        )

        await asyncio.sleep(1)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to NewRelic - Using config at path - {self.config_path} - Environment - {self.environment}')
    
    async def submit_session_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Session System Metrics to NewRelic')

        metrics_sets: List[SessionMetricsCollection] = []
        
        for metrics_set in system_metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Session System Metrics - {metrics_set.system_metrics_set_id}')
            for monitor_metrics in metrics_set.session_cpu_metrics.values():
                metrics_sets.append(monitor_metrics)
                
            for  monitor_metrics in metrics_set.session_memory_metrics.values():
                metrics_sets.append(monitor_metrics)

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Session System Metrics - {metrics_set.name}:{metrics_set.group}')

            for field, value in metrics_set.record.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Session System Metrics - {metrics_set.name}:{metrics_set.group}:{field}')

                record_name = f'{metrics_set.name}_{metrics_set.group}_{field}'

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        record_name,
                        value
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Session System Metrics to NewRelic')

    async def submit_stage_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Stage System Metrics to NewRelic')

        metrics_sets: List[SystemMetricsCollection] = []
        
        for metrics_set in system_metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stage System Metrics - {metrics_set.system_metrics_set_id}')
            
            cpu_metrics = metrics_set.cpu
            memory_metrics = metrics_set.memory

            for stage_name, stage_cpu_metrics in  cpu_metrics.metrics.items():

                for monitor_metrics in stage_cpu_metrics.values():
                    metrics_sets.append(monitor_metrics)

                stage_memory_metrics = memory_metrics.metrics.get(stage_name)
                for monitor_metrics in stage_memory_metrics.values():
                    metrics_sets.append(monitor_metrics)

                stage_mb_per_vu_metrics = metrics_set.mb_per_vu.get(stage_name)
                
                if stage_mb_per_vu_metrics:
                    metrics_sets.append(stage_mb_per_vu_metrics)

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stage System Metrics - {metrics_set.name}:{metrics_set.group}')

            for field, value in metrics_set.record.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stage System Metrics - {metrics_set.name}:{metrics_set.group}:{field}')

                record_name = f'{metrics_set.name}_{metrics_set.group}_{field}'

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        record_name,
                        value
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Session System Metrics to NewRelic')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to NewRelic')

        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped.items():
                for field, value in group.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams - {stage_name}:{group_name}:{field}')
                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.client.record_custom_metric,
                            f'{stage_name}_{group_name}_{field}',
                            value
                        )
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to NewRelic')

    async def submit_experiments(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to NewRelic')

        for experiment in experiments_metrics.experiment_summaries:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.record_custom_event,
                    experiment.experiment_name,
                    experiment.record
                )
            )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to NewRelic')

    async def submit_variants(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to NewRelic')

        for variant in experiments_metrics.variant_summaries:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.record_custom_event,
                    variant.variant_name,
                    variant.record
                )
            )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to NewRelic')

    async def submit_mutations(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to NewRelic')

        for mutation in experiments_metrics.mutation_summaries:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.record_custom_event,
                    mutation.mutation_name,
                    mutation.record
                )
            )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to NewRelic')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to NewRelic')

        for event in events:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.record_custom_event,
                    event.name,
                    event.record
                )
            )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to NewRelic')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to NewRelic')
        
        for metrics_set in metrics_sets:  
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}')
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        f'{metrics_set.name}_{field}',
                        value
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to NewRelic')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to NewRelic')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                for field, value in group.stats.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{field}')
                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.client.record_custom_metric,
                            f'{metrics_set.name}_{group_name}_{field}',
                            value
                        )
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to NewRelic')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to NewRelic')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metric - {metrics_set.name}:custom:{custom_metric_name}')
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        f'{metrics_set.name}_custom_{custom_metric_name}',
                        custom_metric.metric_value
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to NewRelic')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to NewRelic')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                error_message = re.sub(
                    '[^0-9a-zA-Z]+', 
                    '_',
                    error.get(
                        'message'
                    ).lower()
                )

                error_message_metric_name = f'{metrics_set.name}_errors_{error_message}'

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.record_custom_metric,
                        error_message_metric_name,
                        error.get('count')
                    )
                )  
                
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to NewRelic')          

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to NewRelic')

        await self._loop.run_in_executor(
            self._executor,
            self.client.shutdown
        )

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to NewRelic')