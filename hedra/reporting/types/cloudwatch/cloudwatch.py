import asyncio
import functools
import datetime
import json
import uuid
import psutil
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from concurrent.futures import ThreadPoolExecutor
from .cloudwatch_config import CloudwatchConfig

try:
    import boto3
    has_connector = True

except Exception:
    boto3 = None
    has_connector = False


class Cloudwatch:

    def __init__(self, config: CloudwatchConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.iam_role_arn = config.iam_role_arn
        self.schedule_rate = config.schedule_rate

        self.cloudwatch_targets = config.cloudwatch_targets
        self.aws_resource_arns = config.aws_resource_arns
        self.submit_timeout = config.submit_timeout

        self.events_rule_name = config.events_rule
        self.metrics_rule_name = config.metrics_rule
        self.streams_rule_name = config.streams_rule

        self.shared_metrics_rule_name = f'{config.metrics_rule}_shared'
        self.errors_rule_name = f'{config.metrics_rule}_errors'

        self.experiments_rule_name = config.experiments_rule
        self.variants_rule_name = f'{config.experiments_rule}_variants'
        self.mutations_rule_name = f'{config.experiments_rule}_mutations'

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self.experiments_rule = None

        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating AWS Cloudwatch client for region - {self.region_name}')

        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                'events',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created AWS Cloudwatch client for region - {self.region_name}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Cloudwatch rule - {self.streams_rule_name}')

        streams = []
        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stream - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stream Group - {group_name}:{stream.stream_set_id}')
                
                streams.append({
                    'Time': datetime.datetime.now(),
                    'Detail': json.dumps({
                        **group,
                        'group': group_name,
                        'stage': stage_name,
                        'name': f'{stage_name}_streams'
                    }),
                    'DetailType': self.streams_rule_name,
                    'Resources': self.aws_resource_arns,
                    'Source': self.streams_rule_name
                })

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=streams
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Cloudwatch rule - {self.streams_rule_name}')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Cloudwatch rule - {self.experiments_rule_name}')

        cloudwatch_events = [
            {
                'Time': datetime.datetime.now(),
                'Detail': json.dumps(experiment),
                'DetailType': self.experiments_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.experiments_rule_name
            } for experiment in experiment_metrics.experiments
        ]

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=cloudwatch_events
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Cloudwatch rule - {self.experiments_rule_name}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Cloudwatch rule - {self.variants_rule_name}')

        cloudwatch_events = [
            {
                'Time': datetime.datetime.now(),
                'Detail': json.dumps(variant),
                'DetailType': self.variants_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.variants_rule_name
            } for variant in experiment_metrics.variants
        ]

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=cloudwatch_events
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Cloudwatch rule - {self.variants_rule_name}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Cloudwatch rule - {self.mutations_rule_name}')

        cloudwatch_events = [
            {
                'Time': datetime.datetime.now(),
                'Detail': json.dumps(mutation),
                'DetailType': self.mutations_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.mutations_rule_name
            } for mutation in experiment_metrics.mutations
        ]

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=cloudwatch_events
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Cloudwatch rule - {self.mutations_rule_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Cloudwatch rule - {self.events_rule_name}')

        cloudwatch_events = [
            {
                'Time': datetime.datetime.now(),
                'Detail': json.dumps(event.record),
                'DetailType': self.events_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.events_rule_name
            } for event in events
        ]

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=cloudwatch_events
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Cloudwatch rule - {self.events_rule_name}')
    
    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Cloudwatch rule - {self.metrics_rule_name}')

        common_metrics = [
            {
                'Time': datetime.datetime.now(),
                'Detail': json.dumps({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'common',
                    **metrics_set.common_stats
                }),
                'DetailType': self.shared_metrics_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.shared_metrics_rule_name
            } for metrics_set in metrics_sets
        ]

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=common_metrics
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Cloudwatch rule - {self.metrics_rule_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Cloudwatch rule - {self.metrics_rule_name}')

        metrics = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}')
                
                metrics.append({
                    'Time': datetime.datetime.now(),
                    'Detail': json.dumps({
                        **group.record,
                        'group': group_name
                    }),
                    'DetailType': self.metrics_rule_name,
                    'Resources': self.aws_resource_arns,
                    'Source': self.metrics_rule_name
                })

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=metrics
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Cloudwatch rule - {self.metrics_rule_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Cloudwatch rule - {self.metrics_rule_name}')

        custom_metrics = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - Custom')

            custom_metrics.append({
                'Time': datetime.datetime.now(),
                'Detail': json.dumps({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'custom',
                    **{
                        custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    }
                }),
                'DetailType': self.metrics_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.metrics_rule_name
            })

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=custom_metrics
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Cloudwatch rule - {self.metrics_rule_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Cloudwatch rule - {self.errors_rule_name}')

        cloudwatch_errors = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                cloudwatch_errors.append({
                    'Time': datetime.datetime.now(),
                    'Detail': json.dumps(error),
                    'DetailType': self.errors_rule_name,
                    'Resources': self.aws_resource_arns,
                    'Source': self.errors_rule_name
                })

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=cloudwatch_errors
                )
            ),
            timeout=self.submit_timeout
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Cloudwatch rule - {self.errors_rule_name}')


    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
