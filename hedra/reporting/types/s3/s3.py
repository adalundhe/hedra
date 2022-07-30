import asyncio
from datetime import datetime
import functools
import json
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup
from concurrent.futures import ThreadPoolExecutor

try:
    import boto3
    from .s3_config import S3Config
    has_connector = True

except ImportError:
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
        self.errors_bucket_name = f'{self.metrics_bucket_name}-errors'

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
    
    async def submit_metrics(self, metrics: List[MetricsGroup]):

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

        for metrics_group in metrics:
            for timings_group_name, timings_group in metrics_group.groups.items():
                timestamp = int(datetime.now().timestamp())
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=metrics_bucket_name,
                        Key=f'{metrics_group.name}-{timestamp}',
                        Body=json.dumps({
                            **timings_group.record,
                            'timings_group': timings_group_name
                        })
                    )
                )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

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

        for metrics_group in metrics_groups:
            for error in metrics_group.errors:
                timestamp = int(datetime.now().timestamp())
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self.client.put_object,
                        Bucket=errors_bucket_name,
                        Key=f'{metrics_group.name}-{timestamp}',
                        Body=json.dumps({
                            'metrics_name': metrics_group.name,
                            'metrics_stage': metrics_group.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        })
                    )
                )

    async def close(self):
        pass