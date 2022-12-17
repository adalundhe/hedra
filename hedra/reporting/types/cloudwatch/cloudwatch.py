import asyncio
import functools
import datetime
import json
import uuid
from typing import List

import psutil
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet
from concurrent.futures import ThreadPoolExecutor

try:
    import boto3
    from .cloudwatch_config import CloudwatchConfig
    has_connector = True

except Exception:
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
        self.errors_rule_name = 'stage_errors'

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.events_rule = None
        self.metrics_rule= None

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

    async def submit_events(self, events: List[BaseEvent]):

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
                'DetailType': self.events_rule_name,
                'Resources': self.aws_resource_arns,
                'Source': self.metrics_rule_name
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

            for custom_group_name, group in metrics_set.custom_metrics.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {custom_group_name}')

                custom_metrics.append({
                    'Time': datetime.datetime.now(),
                    'Detail': json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': custom_group_name,
                        **group
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
