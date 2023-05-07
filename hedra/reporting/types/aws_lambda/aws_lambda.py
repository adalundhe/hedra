import asyncio
import functools
import json
import uuid
import psutil
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.types import ReporterTypes
from .aws_lambda_config import AWSLambdaConfig

try:
    import boto3
    has_connector=True
except Exception:
    boto3 = None
    has_connector=False


class AWSLambda:

    def __init__(self, config: AWSLambdaConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name

        self.events_lambda_name = config.events_lambda
        self.streams_lambda_name = config.streams_lambda
        
        self.metrics_lambda_name = config.metrics_lambda 
        self.shared_metrics_lambda_name = f'{config.metrics_lambda}_shared'
        self.error_metrics_lambda_name = f'{config.metrics_lambda}_error'

        self.experiments_lambda_name = config.experiments_lambda
        self.variants_lambda_name = f'{config.experiments_lambda}_variants'
        self.mutations_lambda_name = f'{config.experiments_lambda}_mutations'

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._client = None
        self._loop = asyncio.get_event_loop()
        self.session_uuid = str(uuid.uuid4())

        self.reporter_type = ReporterTypes.AWSLambda
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.metadata_string: str = None

        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to AWS - Region: {self.region_name}')

        self._client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                'lambda',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Successfully opened connection to AWS - Region: {self.region_name}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Streams to file - {self.streams_lambda_name}')
        
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.streams_lambda_name,
                Payload=json.dumps([
                    {
                        'stage': stream_name,
                        **stream_set.grouped 
                    } for stream_name, stream_set in stream_metrics.items()
                ])
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Streams to file - {self.streams_lambda_name}')


    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Experiments to file - {self.experiments_lambda_name}')
        
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.experiments_lambda_name,
                Payload=json.dumps([
                    experiment.record for experiment in experiment_metrics.experiment_summaries
                ])
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Experiments to file - {self.experiments_lambda_name}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Variant to file - {self.variants_lambda_name}')
        
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.variants_lambda_name,
                Payload=json.dumps([
                    variant.record for variant in experiment_metrics.variant_summaries
                ])
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Variant to file - {self.variants_lambda_name}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saving Mutation to file - {self.mutations_lambda_name}')
        
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.mutations_lambda_name,
                Payload=json.dumps([
                    mutation.record for mutation in experiment_metrics.mutation_summaries
                ])
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Saved Mutation to file - {self.mutations_lambda_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Lambda - {self.events_lambda_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.events_lambda_name,
                Payload=json.dumps([
                    event.record for event in events
                ])
            )
        )

        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Lambda - {self.events_lambda_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Lambda - {self.shared_metrics_lambda_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.shared_metrics_lambda_name,
                    Payload=json.dumps([
                        {
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': 'common',
                            **metrics_set.common_stats
                        } for metrics_set in metrics_sets
                    ])
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Lambda - {self.shared_metrics_lambda_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Lambda - {self.metrics_lambda_name}')

        for metrics_set in metrics:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._client.invoke,
                    FunctionName=self.metrics_lambda_name,
                    Payload=json.dumps([
                        {
                            'group': group_name,
                            **group.record,
                            **group.custom
                        } for group_name, group in metrics_set.groups.items()
                    ])
                )
            )
            
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Lambda - {self.metrics_lambda_name}')


    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Lambda - {self.metrics_lambda_name}')

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.metrics_lambda_name,
                Payload=json.dumps([
                    {
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'custom',
                        **{
                            metric.metric_shortname: metric.metric_value for metric in metrics_set.custom_metrics.values()
                        }
                    } for metrics_set in metrics_sets
                ])
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Lambda - {self.metrics_lambda_name}')

    async def submit_errors(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics to Lambda - {self.error_metrics_lambda_name}')

        for metrics_set in metrics:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._client.invoke,
                    FunctionName=self.error_metrics_lambda_name,
                    Payload=json.dumps([
                        {
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            **error
                        } for error in metrics_set.errors
                    ])
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics to Lambda - {self.error_metrics_lambda_name}')
            

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')