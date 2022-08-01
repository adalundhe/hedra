import asyncio
import functools
import json
from typing import List
from concurrent.futures import ThreadPoolExecutor

import psutil
from .aws_lambda_config import AWSLambdaConfig
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup

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
        self.metrics_lambda_name = config.metrics_lambda 
        self.group_metrics_lambda_name = f'{self.metrics_lambda_name}_group_metrics'
        self.errors_lambda_name = f'{self.metrics_lambda_name}_errors'   

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
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

    async def submit_events(self, events: List[BaseEvent]):
        for event in events:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._client.invoke,
                    FunctionName=self.events_lambda_name,
                    Payload=json.dumps(event.record)
                )
            )

    async def submit_common(self, metrics_groups: List[MetricsGroup]):
        for metrics_group in metrics_groups:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._client.invoke,
                    FunctionName=self.group_metrics_lambda_name,
                        Payload=json.dumps({
                            'name': metrics_group.name,
                            'stage': metrics_group.stage,
                            **metrics_group.common_stats
                        })
                )
            )

    async def submit_metrics(self, metrics: List[MetricsGroup]):
        for metrics_group in metrics:

            for group_name, group in metrics_group.groups.items():
                
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._client.invoke,
                        FunctionName=self.metrics_lambda_name,
                        Payload=json.dumps({
                            'group': group_name,
                            **group.record,
                            **group.custom
                        })
                    )
                )

    async def submit_errors(self, metrics: List[MetricsGroup]):
        for metrics_group in metrics:
            for error in metrics_group.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._client.invoke,
                        FunctionName=self.errors_lambda_name,
                        Payload=json.dumps({
                            'name': metrics_group.name,
                            'stage': metrics_group.stage,
                            **error
                        })
                    )
                )

            

    async def close(self):
        pass
