import asyncio
from datetime import datetime
import functools
import json
from typing import List

import psutil
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
        self.stage_metrics_bucket_name = 'stage-metrics'
        self.errors_bucket_name = 'stage-errors'

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
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

    async def submit_events(self, events: List[BaseEvent]):
        
        events_bucket_name = f'{self.buckets_namespace}-{self.events_bucket_name}'

        try:
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

        except Exception:
            pass


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

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        stage_metrics_bucket_name = f'{self.buckets_namespace}-{self.stage_metrics_bucket_name}'

        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=stage_metrics_bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region_name
                    }
                )
            )

        except Exception:
            pass
        
        for metrics_set in metrics_sets:
            timestamp = int(datetime.now().timestamp())
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_object,
                    Bucket=stage_metrics_bucket_name,
                    Key=f'{metrics_set.name}-{timestamp}',
                    Body=json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        **metrics_set.common_stats
                    })
                )
            )
    
    async def submit_metrics(self, metrics: List[MetricsSet]):

        metrics_bucket_name = f'{self.buckets_namespace}-{self.metrics_bucket_name}'
        
        try:
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

        except Exception:
            pass

        for metrics_set in metrics:
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

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_bucket_name = f'{self.buckets_namespace}-{custom_group_name}-metrics'
                
                try:
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

                except Exception:
                    pass

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

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        errors_bucket_name = f'{self.buckets_namespace}-{self.errors_bucket_name}'

        try:
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

        except Exception:
            pass

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

    async def close(self):
        pass