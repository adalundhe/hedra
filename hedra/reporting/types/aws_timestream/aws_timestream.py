import asyncio
import functools
import uuid
import psutil
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric
from .aws_timestream_config import AWSTimestreamConfig
from .aws_timestream_record import AWSTimestreamRecord

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

    async def submit_metrics(self, metrics: List[Metric]):

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

        for metric in metrics:
            for field, value in metric.stats.items():
                timestream_record = AWSTimestreamRecord(
                    'metric',
                    metric.name,
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
                TableName=self.metrics_table_name,
                Records=records,
                CommonAttributes={}
            )
        )

    async def close(self):
        pass