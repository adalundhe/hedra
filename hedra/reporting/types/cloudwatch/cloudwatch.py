import asyncio
import functools
import datetime
import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


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
        self.iam_role = config.iam_role
        self.schedule_rate = config.schedule_rate
        self.cloudwatch_targets = config.cloudwatch_targets
        self.aws_resource_arns = config.aws_resource_arns
        self.cloudwatch_source = config.cloudwatch_source

        self.events_rule = None
        self.metrics_rule= None

        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            None,
            functools.partial(
                boto3.client,
                'events',
                aws_access_key_id=self.aws_access_key_id,
                aws_sercret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.put_rule,
                Name=self.events_rule,
                RoleArn=self.iam_role,
                ScheduleExpression=self.schedule_rate,
                State='ENABLED'
            )
        )

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.put_rule,
                Name=self.metrics_rule,
                RoleArn=self.iam_role,
                ScheduleExpression=self.schedule_rate,
                State='ENABLED'
            )
        )

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.put_targets,
                Rule=self.events_rule,
                Targets=[
                    {
                        'Arn': target.get('arn'),
                        'Id': target.get('id')
                    } for target in self.cloudwatch_targets
                ]
            )
        )

        await self._loop.run_in_executor(
            None,
            functools.partial(
                self.client.put_targets,
                Rule=self.metrics_rule,
                Targets=[
                    {
                        'Arn': target.get('arn'),
                        'Id': target.get('id')
                    } for target in self.cloudwatch_targets
                ]
            )
        )

    async def submit_events(self, events: List[BaseEvent]):                
        for event in events:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.put_events,
                    Entries=[
                        {
                            'Time': datetime.datetime.now(),
                            'Detail': json.dumps(event.record),
                            'DetailType': self.events_rule,
                            'Resources': self.aws_resource_arns,
                            'Source': self.cloudwatch_source
                        }
                    ]
                )
            )

    async def submit_metrics(self, metrics: List[Metric]):
        for metric in metrics:
            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self.client.put_events,
                    Entries=[
                        {
                            'Time': datetime.datetime.now(),
                            'Detail': json.dumps(metric.record),
                            'DetailType': self.metrics_rule,
                            'Resources': self.aws_resource_arns,
                            'Source': self.cloudwatch_source
                        }
                    ]
                )
            )

    async def close(self):
        pass
