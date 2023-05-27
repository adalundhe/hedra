import uuid
from numpy import float32, float64, int16, int32, int64
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import (
    MetricsSet,
    MetricType
)
from hedra.reporting.system.system_metrics_set import (
    SystemMetricsSet,
    SessionMetricsCollection,
    SystemMetricsCollection
)
from typing import Dict
from .datadog_config import DatadogConfig

try:
    # Datadog uses aiosonic
    from aiosonic import HTTPClient, TCPConnector, Timeouts
    from datadog_api_client import (
        AsyncApiClient, 
        Configuration
    )
    from datadog_api_client.v2.api.metrics_api import (MetricsApi, MetricPayload)
    from datadog_api_client.v2.model.metric_series import MetricSeries
    from datadog_api_client.v2.model.metric_point import MetricPoint

    from datadog_api_client.v1.api.events_api import EventsApi
    from datadog_api_client.v1.api.events_api import EventCreateRequest
    has_connector = True

except Exception:
    datadog = None
    has_connector = False

from datetime import datetime
from typing import List


class Datadog:

    def __init__(self, config: DatadogConfig) -> None:
        self.datadog_api_key = config.api_key
        self.datadog_app_key = config.app_key
        self.event_alert_type = config.event_alert_type or 'info'
        self.device_name = config.device_name or 'hedra'
        self.priority = config.priority
        self.custom_fields = config.custom_fields or {}

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.types_map = {
            'total': 'count',
            'succeeded': 'count',
            'failed': 'count',
            'actions_per_second': 'gauge',
            'median': 'gauge',
            'mean': 'gauge',
            'variance': 'gauge',
            'stdev': 'gauge',
            'minimum': 'gauge',
            'maximum': 'gauge',
            **self.custom_fields
        }
        
        self._datadog_api_map = {
            'count': 1,
            'rate': 2,
            'gauge': 3,
            'histogram': 4,
        }

        self._config = None
        self._client = None
        self.events_api = None
        self.metrics_api = None
        self.metric_types_map = {
            MetricType.COUNT: 'count',
            MetricType.RATE: 'rate',
            MetricType.DISTRIBUTION: 'histogram',
            MetricType.SAMPLE: 'gauge'
        }

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Datadogg API')

        self._config = Configuration()
        self._config.api_key["apiKeyAuth"] = self.datadog_api_key
        self._config.api_key["appKeyAuth"] = self.datadog_app_key

        self._client = AsyncApiClient(self._config)

        # Datadog's implementation of aiosonic's HTTPClient lacks a lot
        # of configurability, incuding actually being able to set request timeouts
        # so we substitute our own implementation.

        tcp_connection = TCPConnector(timeouts=Timeouts(sock_connect=30))
        self._client.rest_client._client = HTTPClient(tcp_connection)

        self.events_api = EventsApi(self._client)
        self.metrics_api = MetricsApi(self._client)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Datadogg API')

    async def submit_session_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Session System Metrics to Datadog API')

        metrics_sets: List[SessionMetricsCollection] = []
        for metrics_set in system_metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Session System Metrics - {metrics_set.system_metrics_set_id}')
            
            for monitor_metrics in metrics_set.session_cpu_metrics.values():
                metrics_sets.append(monitor_metrics)
                
            for  monitor_metrics in metrics_set.session_memory_metrics.values():
                metrics_sets.append(monitor_metrics)

        system_session_metrics_series: List[MetricSeries] = []

        for metrics_set in metrics_sets:

            tags = [
                f'group:{metrics_set.group}'
            ]

            for field, value in metrics_set.stats.items():

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Session System Metric - {metrics_set.name}:{metrics_set.group}:{field}')

                metric_type = metrics_set.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type)

                if isinstance(value, (int, int16, int32, int64)):
                    value = int(value)

                elif isinstance(value, (float, float32, float64)):
                    value = float(value)

                series = MetricSeries(
                    f'{metrics_set.name}_{metrics_set.group}_{field}', 
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=value
                    )],
                    type=datadog_metric_type,
                    tags=tags
                )

                system_session_metrics_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(system_session_metrics_series))
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Session System Metrics to Datadog API')

    async def submit_stage_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Stage System Metrics to Datadog API')

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

        system_stage_metrics_series: List[MetricSeries] = []

        for metrics_set in metrics_sets:

            tags = [
                f'group:{metrics_set.group}'
            ]

            for field, value in metrics_set.stats.items():

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Stage System Metric - {metrics_set.name}:{metrics_set.group}:{field}')

                metric_type = metrics_set.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type)

                if isinstance(value, (int, int16, int32, int64)):
                    value = int(value)

                elif isinstance(value, (float, float32, float64)):
                    value = float(value)

                series = MetricSeries(
                    f'{metrics_set.name}_{metrics_set.group}_{field}', 
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=value
                    )],
                    type=datadog_metric_type,
                    tags=tags
                )

                system_stage_metrics_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(system_stage_metrics_series))
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Stage System Metrics to Datadog API')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Datadog API')

        streams_series = []
        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stream - {stage_name}:{stream.stream_set_id}')
            
            for group_name, group in stream.grouped.items():
                tags = [
                    f'stage:{stage_name}',
                    f'group:{group}'
                ]

                for field, value in group.items():

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Stream Metric - {stage_name}:{group_name}:{field}')

                    metric_type = stream.types_map.get(field)
                    datadog_metric_type = self._datadog_api_map.get(metric_type)

                    if isinstance(value, (int, int16, int32, int64)):
                        value = int(value)

                    elif isinstance(value, (float, float32, float64)):
                        value = float(value)

                    series = MetricSeries(
                        f'{stage_name}_{group}_{field}', 
                        [MetricPoint(
                            timestamp=int(datetime.now().timestamp()),
                            value=value
                        )],
                        type=datadog_metric_type,
                        tags=tags
                    )

                    streams_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(streams_series))
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Datadog API')        

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Datadog API')

        experiments_series = []
        for experiment in experiment_metrics.experiment_summaries:

            experiment_id = uuid.uuid4()

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Experiment - {experiment.experiment_name}:{experiment_id}')

            tags = [
                f'experiment_name:{experiment.experiment_name}',
                f'experiment_randomized:{experiment.experiment_randomized}'
            ]

            for field, value in experiment.stats:

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Experiments - {experiment.experiment_name}:{field}')

                metric_type = experiment.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type.value)

                series = MetricSeries(
                    f'{experiment.experiment_name}_{field}', 
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=value
                    )],
                    type=datadog_metric_type,
                    tags=tags
                )

                experiments_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(experiments_series))
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Datadog API')
    
    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Datadog API')

        variant_series = []
        for variant in experiment_metrics.variant_summaries:

            variant_id = uuid.uuid4()

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Variants - {variant.variant_name}:{variant_id}')

            tags = [
                f'variant_name:{variant.variant_name}',
                f'variant_experiment:{variant.variant_experiment}',
                f'variant_distribution:{variant.variant_distribution}',
            ]

            for field, value in variant.stats:

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Variant - {variant.variant_name}:{field}')

                metric_type = variant.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type.value)

                series = MetricSeries(
                    f'{variant.variant_name}_{field}', 
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=value
                    )],
                    type=datadog_metric_type,
                    tags=tags
                )

                variant_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(variant_series))
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Datadog API')
    
    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Datadog API')

        mutation_series = []
        for mutation in experiment_metrics.mutation_summaries:

            mutation_id = uuid.uuid4()

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Mutations - {mutation.mutation_name}:{mutation_id}')

            tags = [
                f'mutation_name:{mutation.mutation_name}',
                f'mutation_experiment_name:{mutation.mutation_experiment_name}',
                f'mutation_variant_name:{mutation.mutation_variant_name}',
                f'mutation_targets:{mutation.mutation_targets}',
                f'mutation_type:{mutation.mutation_type}',
            ]

            for field, value in mutation.stats:

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Mutation - {mutation.mutation_name}:{field}')

                metric_type = mutation.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type.value)

                series = MetricSeries(
                    f'{mutation.mutation_name}_{field}', 
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=value
                    )],
                    type=datadog_metric_type,
                    tags=tags
                )

                mutation_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(mutation_series))
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Datadog API')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Datadog API')

        for event in events:

            tags = {
                f'{tag.name}:{tag.value}' for tag in event.tags
            }

            await self.events_api.create_event(
                EventCreateRequest(
                    title=event.name,
                    text=event.serialize(),
                    alert_type=self.event_alert_type,
                    aggregation_key=event.type,
                    device_name=self.device_name,
                    date_happened=datetime.now().strftime('%Y-%m-%dT%H:%M:%S.Z'),
                    priority=self.priority,
                    tags=tags,
                    host=event.hostname
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Datadog API')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Datadog API')

        metrics_series = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            tags = [
                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
            ]

            for field, value in metrics_set.common_stats.items():

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metric - {metrics_set.name}:common:{field}')

                metric_type = self.types_map.get(field)
                datadog_metric_type = self._datadog_api_map.get(metric_type)

                if isinstance(value, (int, int16, int32, int64)):
                    value = int(value)

                elif isinstance(value, (float, float32, float64)):
                    value = float(value)

                series = MetricSeries(
                    f'{metrics_set.name}_{field}', 
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=value
                    )],
                    type=datadog_metric_type,
                    tags=[
                        *tags,
                        f'metric_stage:{metrics_set.stage}',
                        f'group:common'
                    ]
                )

                metrics_series.append(series)
                
        await self.metrics_api.submit_metrics(MetricPayload(metrics_series))
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Datadog API')        

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Datadog API')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            tags = [
                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
            ]

            metrics_series = []
            for group_name, group in metrics_set.groups.items():

                for field, value in group.stats.items():

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metric - {metrics_set.name}:{group_name}:{field}')

                    metric_type = self.types_map.get(field)
                    datadog_metric_type = self._datadog_api_map.get(metric_type)

                    if isinstance(value, (int, int16, int32, int64)):
                        value = int(value)

                    elif isinstance(value, (float, float32, float64)):
                        value = float(value)

                    series = MetricSeries(
                        f'{metrics_set.name}_{field}', 
                        [MetricPoint(
                            timestamp=int(datetime.now().timestamp()),
                            value=float(value)
                        )],
                        type=datadog_metric_type,
                        tags=[
                            *tags,
                            f'metric_stage:{metrics_set.stage}',
                            f'group:{group_name}'
                        ]
                    )

                    metrics_series.append(series)

                for quantile_name, quantile_value in group.quantiles.items():

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metric Quantile - {metrics_set.name}:{group_name}:{quantile_name}th Quantile')

                    if isinstance(quantile_value, (int, int16, int32, int64)):
                        quantile_value = int(quantile_value)

                    elif isinstance(quantile_value, (float, float32, float64)):
                        quantile_value = float(quantile_value)

                    datadog_metric_type = self._datadog_api_map.get('gauge')
                    series = MetricSeries(
                        f'{metrics_set.name}_{quantile_name}', 
                        [MetricPoint(
                            timestamp=int(datetime.now().timestamp()),
                            value=float(quantile_value)
                        )],
                        type=datadog_metric_type,
                        tags=[
                            *tags,
                            f'metric_stage:{metrics_set.stage}',
                            f'group:{group_name}'
                        ]
                    )

                    metrics_series.append(series)
     
            await self.metrics_api.submit_metrics(MetricPayload(metrics_series))

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Datadog API')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Datadog API')

        metrics_series = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            tags = [
                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
            ]

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():

                metric_type = self.metric_types_map.get(
                    custom_metric.metric_type,
                    'gauge'
                )

                datadog_metric_type = self._datadog_api_map.get(metric_type)

                series = MetricSeries(
                    f'{metrics_set.name}_{custom_metric_name}',
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=custom_metric.metric_name
                    )],
                    type=datadog_metric_type,
                    tags=[
                        *tags,
                        f'metric_stage:{metrics_set.stage}',
                        'group:custom'
                    ]
                )

                metrics_series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(metrics_series))

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Datadog API')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Datadog API')

        error_series = [] 
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            tags = [
                f'{tag.name}:{tag.value}' for tag in metrics_set.tags
            ]

            for error in metrics_set.errors:
                
            
                error_message = error.get('error_message')

                series = MetricSeries(
                    f'{metrics_set.name}_errors',
                    [MetricPoint(
                        timestamp=int(datetime.now().timestamp()),
                        value=int(error.get('count'))
                    )],
                    type=self._datadog_api_map.get('count'),
                    tags=[
                        *tags,
                        f'metric_stage:{metrics_set.stage}',
                        f'error_message:{error_message}'
                    ]
                )
               
                error_series.append(series)

        await self.metrics_api.submit_metrics(MetricPayload(error_series))

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Datadog API')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')