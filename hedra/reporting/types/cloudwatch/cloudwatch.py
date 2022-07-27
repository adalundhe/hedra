import asyncio
import functools
import datetime
import json
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric
from concurrent.futures import ThreadPoolExecutor

try:
    import boto3
    from .cloudwatch_config import CloudwatchConfig
    has_connector = True

except ImportError:
    boto3 = None
    CloudwatchConfig = None
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

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.events_rule = None
        self.metrics_rule= None

        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
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

    async def submit_events(self, events: List[BaseEvent]):

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

    async def submit_metrics(self, metrics: List[Metric]):

        cloudwatch_metrics = [
            {
                'Time': datetime.datetime.now(),
                'Detail': json.dumps(metric.record),
                'DetailType': self.metrics_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.metrics_rule_name
            } for metric in metrics
        ]

        await asyncio.wait_for(
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.put_events,
                    Entries=cloudwatch_metrics
                )
            ),
            timeout=self.submit_timeout
        )

    async def close(self):
        pass
