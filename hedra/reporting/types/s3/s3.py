import asyncio
import functools
import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


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
        self.events_bucket = None
        self.metrics_bucket= None
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                boto3.client,
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_sercret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

    async def submit_events(self, events: List[BaseEvent]):
        
        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=self.events_bucket
                )
            )

        except Exception:
            pass


        for event in events:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.put_object,
                    Bucket=self.events_bucket,
                    Key=event.name,
                    Body=json.dumps(event.record)
                )
            )
    
    async def submit_metrics(self, metrics: List[Metric]):
        
        try:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.create_bucket,
                    Bucket=self.metrics_bucket
                )
            )

        except Exception:
            pass


        for metric in metrics:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.put_object,
                    Bucket=self.metrics_bucket,
                    Key=metric.name,
                    Body=json.dumps(metric.record)
                )
            )

    async def close(self):
        pass