import asyncio
import uuid
import functools
import json
import psutil
from datetime import datetime
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from concurrent.futures import ThreadPoolExecutor
from .s3_config import S3Config

try:
    import boto3
    has_connector = True

except Exception:
    boto3 = None
    has_connector = False


class S3:

    def __init__(self, config: S3Config) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.buckets_namespace = config.buckets_namespace

        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.streams_bucket_name = config.streams_bucket

        self.experiments_bucket_name = config.experiments_bucket
        self.variants_bucket_name = f'{config.experiments_bucket}_variants'
        self.mutations_bucket_name = f'{config.experiments_bucket}_mutations'

        self.shared_metrics_bucket_name = f'{config.metrics_bucket}_shared'
        self.errors_bucket_name = f'{config.metrics_bucket}_errors'
        self.custom_metrics_bucket_name = f'{config.metrics_bucket}_custom'

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to AWS S3 - Region: {self.region_name}')
        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to AWS S3 - Region: {self.region_name}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        streams_bucket_name = f'{self.buckets_namespace}-{self.streams_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Bucket - {streams_bucket_name}')
        
        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Streams Bucket - {streams_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=streams_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Streams Bucket - {streams_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Streams Bucket - {streams_bucket_name}')

        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams Set - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped.items():
                streams_set_key = f'{stage_name}_{group_name}_{stream.stream_set_id}'
                
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=streams_bucket_name,
                        Key=streams_set_key,
                        Body=json.dumps({
                            **group.record,
                            'group': group_name,
                            'stage': stage_name,
                            'name': f'{stage_name}_streams'
                        })
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Bucket - {streams_bucket_name}')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        experiments_bucket_name = f'{self.buckets_namespace}-{self.experiments_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Bucket - {experiments_bucket_name}')

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Experiments Bucket - {experiments_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=experiments_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Experiments Bucket - {experiments_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Experiments Bucket - {experiments_bucket_name}')
        
        for experiment in experiment_metrics.experiment_summaries:
            
            experiment_id = uuid.uuid4()

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Experiment - {experiment.experiment_name}:{experiment_id}')
      
            experiment_key = f'{experiment.experiment_name}_{experiment_id}'

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=experiments_bucket_name,
                    Key=experiment_key,
                    Body=json.dumps(experiment.record)
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Bucket - {experiments_bucket_name}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        variants_bucket_name = f'{self.buckets_namespace}-{self.variants_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Bucket - {variants_bucket_name}')

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Variants Bucket - {variants_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=variants_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Variants Bucket - {variants_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Variants Bucket - {variants_bucket_name}')
        
        for variant in experiment_metrics.variant_summaries:
            
            variant_id = uuid.uuid4()

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Variant - {variant.variant_name}:{variant_id}')
      
            variant_key = f'{variant.variant_name}_{variant_id}'

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=variants_bucket_name,
                    Key=variant_key,
                    Body=json.dumps(variant.record)
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Bucket - {variants_bucket_name}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        mutations_bucket_name = f'{self.buckets_namespace}-{self.mutations_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Bucket - {mutations_bucket_name}')

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Mutations Bucket - {mutations_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=mutations_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Mutations Bucket - {mutations_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Mutations Bucket - {mutations_bucket_name}')
        
        for mutation in experiment_metrics.mutation_summaries:
            
            mutation_id = uuid.uuid4()

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Mutation - {mutation.mutation_name}:{mutation_id}')
      
            mutation_key = f'{mutation.mutation_name}_{mutation_id}'

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=mutations_bucket_name,
                    Key=mutation_key,
                    Body=json.dumps(mutation.record)
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Bucket - {mutations_bucket_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):
        
        events_bucket_name = f'{self.buckets_namespace}-{self.events_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Bucket - {events_bucket_name}')

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Events Bucket - {events_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=events_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Events Bucket - {events_bucket_name} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Events Bucket - {events_bucket_name}')


        for event in events:
            event_key = f'{event.name}_{event.event_id}'

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=events_bucket_name,
                    Key=event_key,
                    Body=json.dumps(event.record)
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Bucket - {events_bucket_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        shared_metrics_bucket_name = f'{self.buckets_namespace}-{self.shared_metrics_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Bucket - {shared_metrics_bucket_name}')

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Shared Metrics Bucket - {shared_metrics_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=shared_metrics_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Shared Metrics Bucket - {shared_metrics_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Shared Metrics Bucket - {shared_metrics_bucket_name}')
        
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
      
            metrics_set_key = f'{metrics_set.name}_{metrics_set.metrics_set_id}'

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=shared_metrics_bucket_name,
                    Key=metrics_set_key,
                    Body=json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        **metrics_set.common_stats
                    })
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Bucket - {shared_metrics_bucket_name}')
    
    async def submit_metrics(self, metrics: List[MetricsSet]):

        metrics_bucket_name = f'{self.buckets_namespace}-{self.metrics_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Bucket - {metrics_bucket_name}')
        
        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Metrics Bucket - {metrics_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=metrics_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Metrics Bucket - {metrics_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Metrics Bucket - {metrics_bucket_name}')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                metrics_set_key = f'{metrics_set.name}_{group_name}_{group.metrics_group_id}'
                
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=metrics_bucket_name,
                        Key=metrics_set_key,
                        Body=json.dumps({
                            **group.record,
                            'group': group_name
                        })
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Bucket - {metrics_bucket_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Custom Metrics Bucket - {self.custom_metrics_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=self.custom_metrics_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Custom Metrics Bucket - {self.custom_metrics_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Custom Metrics Bucket - {self.custom_metrics_bucket_name}')


        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
               
            custom_metric_key = f'{metrics_set.name}_custom_{metrics_set.metrics_set_id}'

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=self.custom_metrics_bucket_name,
                    Key=custom_metric_key,
                    Body=json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'custom',
                        **{
                            custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                        }
                    })
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Bucket - {self.custom_metrics_bucket_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        errors_bucket_name = f'{self.buckets_namespace}-{self.errors_bucket_name}'
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Bucket - {errors_bucket_name}')

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Error Metrics Bucket - {errors_bucket_name} - if not exists')
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=errors_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Error Metrics Bucket - {errors_bucket_name}')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Error Metrics Bucket - {errors_bucket_name}')

        for metrics_set in metrics_sets:
            for error in metrics_set.errors:
                timestamp = int(datetime.now().timestamp())
                metric_key = f'{metrics_set.name}-{timestamp}'

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=errors_bucket_name,
                        Key=metric_key,
                        Body=json.dumps({
                            'metrics_name': metrics_set.name,
                            'metrics_stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        })
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Bucket - {errors_bucket_name}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
