import asyncio
import functools
import uuid
import psutil
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup
from .aws_timestream_config import AWSTimestreamConfig
from .aws_timestream_record import AWSTimestreamRecord
from .aws_timestream_error_record import AWSTimestreamErrorRecord

try:

    import boto3
    has_connector = True
except ImportError:
    has_connector = False


class AWSTimestream:

    def __init__(self, config: AWSTimestreamConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.database_name = config.database_name
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.retention_options = config.retention_options
        self.session_uuid = str(uuid.uuid4())

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):
        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                'timestream-write',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_database,
                    DatabaseName=self.database_name
                )
            )
        
        except Exception:
            pass

    async def submit_events(self, events: List[BaseEvent]):

        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.events_table_name,
                    RetentionProperties=self.retention_options
                )
            )

        except Exception:
            pass

        records = []

        for event in events:
            for field, value in event.record.items():
                timestream_record = AWSTimestreamRecord(
                    'event',
                    event.name,
                    field,
                    value,
                    self.session_uuid
                )

                records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.events_table_name,
                Records=records,
                CommonAttributes={}
            )
        )

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        try:
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.metrics_table_name,
                    RetentionProperties=self.retention_options
                )
            )

        except Exception:
            pass

        records = []

        for metrics_group in metrics:

            for timings_group_name, timings_group in metrics_group.groups.items():
            
                metric_result = {**timings_group.stats, **timings_group.custom}

                for field, value in metric_result.items():
                    timestream_record = AWSTimestreamRecord(
                        'metric',
                        metrics_group.name,
                        metrics_group.stage,
                        timings_group_name,
                        field,
                        value,
                        self.session_uuid,
                    )

                    records.append(timestream_record.to_dict())
        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=self.metrics_table_name,
                Records=records,
                CommonAttributes={}
            )
        )
        
    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        try:

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=f'{self.metrics_table_name}_errors',
                    RetentionProperties=self.retention_options
                )
            )


        except Exception:
            pass

        error_records = []

        for metrics_group in metrics_groups:
            for error in metrics_group.errors:
                timestream_record = AWSTimestreamErrorRecord(
                    'metric',
                    metrics_group.name,
                    metrics_group.stage,
                    error.get('message'),
                    error.get('count'),
                    self.session_uuid
                )

                error_records.append(timestream_record.to_dict())

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self.client.write_records,
                DatabaseName=self.database_name,
                TableName=f'{self.metrics_table_name}_errors',
                Records=error_records,
                CommonAttributes={}
            )
        )


    async def close(self):
        pass