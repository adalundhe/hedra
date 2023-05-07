import uuid
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from .influxdb_config import InfluxDBConfig


try:
    from influxdb_client import Point
    from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
    has_connector = True

except Exception:
    Point = None
    InfluxDBClientAsync = None
    has_connector = False


class InfluxDB:

    def __init__(self, config: InfluxDBConfig) -> None:
        self.host = config.host
        self.token = config.token
        self.protocol = 'https' if config.secure else 'http'
        self.organization = config.organization
        self.connect_timeout = config.connect_timeout

        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.streams_bucket_name = config.streams_bucket
        self.shared_metrics_bucket_name = f'{config.metrics_bucket}_shared'

        self.experiments_bucket_name = config.experiments_bucket
        self.variants_bucket_name = f'{config.experiments_bucket}_variants'
        self.mutations_bucket_name = f'{config.experiments_bucket}_mutations'

        self.errors_bucket_name = f'{config.metrics_bucket}_errors'
        self.custom_bucket_name = f'{config.metrics_bucket}_custom'

        self.client = None
        self.write_api = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')

        self.client = InfluxDBClientAsync(
            f'{self.protocol}://{self.host}',
            token=self.token,
            org=self.organization,
            timeout=self.connect_timeout
        )

        self.write_api = self.client.write_api()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')

    async def submit_streams(self, stage_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Bucket - {self.streams_bucket_name}')

        points = []
        for stage_name, stream in stage_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams - {stage_metrics}:{stream.stream_set_id}')
            
            stream_name = f'{stage_name}_streams'

            for group_name, group in stream.grouped.items():
                point = Point(stream_name)
                tags = [
                    ("name", stream_name),
                    ("stage", stage_name),
                    ("group", group_name)
                ]

                for tag_name, tag_value in tags:
                    point.tag(tag_name, tag_value)

                for field, value in group.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stream - {stage_name}:{group_name}:{field}')
                    point.field(field, value)

                points.append(point)

        await self.write_api.write(
            bucket=self.streams_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Bucket - {self.streams_bucket_name}')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Bucket - {self.experiments_bucket_name}')

        points = []
        for experiment in experiment_metrics.experiment_summaries:
            point = Point(experiment.experiment_name)


            for tag in experiment.tags:
                point.tag(tag.name, tag.value)

            for field, value in experiment.stats.items():
                point.field(f'{experiment.experiment_name}_{field}', value)

            points.append(point)

        await self.write_api.write(
            bucket=self.experiments_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Bucket - {self.experiments_bucket_name}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Bucket - {self.variants_bucket_name}')

        points = []
        for variant in experiment_metrics.variant_summaries:
            point = Point(variant.variant_name)


            for tag in variant.tags:
                point.tag(tag.name, tag.value)

            for field, value in variant.stats.items():
                point.field(f'{variant.variant_name}_{field}', value)

            points.append(point)

        await self.write_api.write(
            bucket=self.variants_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Bucket - {self.variants_bucket_name}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Bucket - {self.mutations_bucket_name}')

        points = []
        for mutation in experiment_metrics.mutation_summaries:
            point = Point(mutation.mutation_name)


            for tag in mutation.tags:
                point.tag(tag.name, tag.value)

            for field, value in mutation.stats.items():
                point.field(f'{mutation.mutation_name}_{field}', value)

            points.append(point)

        await self.write_api.write(
            bucket=self.mutations_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Bucket - {self.mutations_bucket_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Bucket - {self.events_bucket_name}')

        points = []
        for event in events:
            point = Point(event.name)


            for tag in event.tags:
                point.tag(tag.name, tag.value)

            point.field(f'{event.name}_time', event.time)

            if event.success:
                point.field(f'{event.name}_success', 1)
            
            else:
                point.field(f'{event.name}_failed', 1)

            points.append(point)

        await self.write_api.write(
            bucket=self.events_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Bucket - {self.events_bucket_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Bucket - {self.shared_metrics_bucket_name}')

        points = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            point = Point(metrics_set.name)

            for tag in metrics_set.tags:
                point.tag(tag.name, tag.value)

            metric_record = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                **metrics_set.common_stats
            }

            for field, value in metric_record.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}')
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self.shared_metrics_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Bucket - {self.shared_metrics_bucket_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Bucket - {self.metrics_bucket_name}')

        points = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            for group_name, group in metrics_set.groups.items():
                point = Point(metrics_set.name)

                for tag in metrics_set.tags:
                    point.tag(tag.name, tag.value)

                metric_record = {
                    **group.stats, 
                    **group.custom,
                    'group': group_name
                }

                for field, value in metric_record.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:{group_name}:{field}')
                    point.field(field, value)

                points.append(point)

        await self.write_api.write(
            bucket=self.metrics_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Bucket - {self.metrics_bucket_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        points = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():

                point = Point(f'{metrics_set.name}_{custom_metric_name}')

                for tag in metrics_set.tags:
                    point.tag(tag.name, tag.value)
                
                metric_record = {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'custom',
                    custom_metric_name: custom_metric.metric_value
                }

                for field, value in metric_record.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:custom:{field}')
                    point.field(field, value)

                points.append(point)

        for point in points:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Bucket - {self.custom_bucket_name}_metrics')
            await self.write_api.write(
                bucket=self.custom_bucket_name,
                record=points
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Bucket - {self.custom_bucket_name}_metrics')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Errors Metrics to Bucket - {self.errors_bucket_name}')

        points = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                point = Point(f'{metrics_set.name}_errors')
                point.field(
                    error.get('message'),
                    error.get('count')
                )

            points.append(point)

        await self.write_api.write(
            bucket=self.errors_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics to Bucket - {self.errors_bucket_name}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')
        
        await self.client.close()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')
