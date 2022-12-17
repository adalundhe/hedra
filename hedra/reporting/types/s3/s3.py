import asyncio
import uuid
import functools
import json
import psutil
from datetime import datetime
from typing import List
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet
from concurrent.futures import ThreadPoolExecutor

try:
    import boto3
    from .s3_config import S3Config
    has_connector = True

except Exception:
    boto3 = None
    S3Config = None
    has_connector = False


class S3:

    def __init__(self, config: S3Config) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.buckets_namespace = config.buckets_namespace
        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.shared_metrics_bucket_name = 'stage-metrics'
        self.errors_bucket_name = 'stage-errors'

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

    async def submit_events(self, events: List[BaseEvent]):
        
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
            timestamp = int(datetime.now().timestamp())
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=events_bucket_name,
                    Key=f'{event.name}-{timestamp}',
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
            
            timestamp = int(datetime.now().timestamp())
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=shared_metrics_bucket_name,
                    Key=f'{metrics_set.name}-{timestamp}',
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
                timestamp = int(datetime.now().timestamp())
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=metrics_bucket_name,
                        Key=f'{metrics_set.name}-{timestamp}',
                        Body=json.dumps({
                            **group.record,
                            'group': group_name
                        })
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Bucket - {metrics_bucket_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_bucket_name = f'{self.buckets_namespace}-{custom_group_name}-metrics'
                
                try:
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Custom Metrics Bucket - {custom_bucket_name} - if not exists')
                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            self.client.create_bucket,
                            Bucket=custom_bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': self.region_name
                            }
                        )
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created Custom Metrics Bucket - {custom_bucket_name}')

                except Exception:
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of Custom Metrics Bucket - {custom_bucket_name}')

                timestamp = int(datetime.now().timestamp())
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=custom_bucket_name,
                        Key=f'{metrics_set.name}-{custom_group_name}-{timestamp}',
                        Body=json.dumps({
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Bucket - {custom_bucket_name}')

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
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=errors_bucket_name,
                        Key=f'{metrics_set.name}-{timestamp}',
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
