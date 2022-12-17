import asyncio
import functools
import uuid
import psutil
from typing import List
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet
from .aws_timestream_config import AWSTimestreamConfig
from .aws_timestream_record import AWSTimestreamRecord
from .aws_timestream_error_record import AWSTimestreamErrorRecord

try:

    import boto3
    has_connector = True
except Exception:
    has_connector = False


class AWSTimestream:

    def __init__(self, config: AWSTimestreamConfig) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name

        self.database_name = config.database_name
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.stage_metrics_table_name = 'stage_metrics'
        self.errors_table_name = 'stage_errors'

        self.retention_options = config.retention_options
        self.session_uuid = str(uuid.uuid4())

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()
        self.metadata_string: str = None

        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to AWS - Region: {self.region_name}')

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

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Database: {self.database_name} - if not exists')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_database,
                    DatabaseName=self.database_name
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Database: {self.database_name} - if not exists')
        
        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of table - Database: {self.database_name} - if not exists')
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Successfully opened connection to AWS - Region: {self.region_name}')

    async def submit_events(self, events: List[BaseEvent]):

        try:
            
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.events_table_name,
                    RetentionProperties=self.retention_options
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of table - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

        records = []
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

        for event in events:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Event - {event.name}:{event.event_id}')

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

    async def submit_common(self, metrics: List[MetricsSet]):
        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Database: {self.database_name} - Table: {self.metrics_table_name} - if not exists')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.stage_metrics_table_name,
                    RetentionProperties=self.retention_options
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Database: {self.database_name} - Table: {self.metrics_table_name} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of table - Database: {self.database_name} - Table: {self.metrics_table_name} - if not exists')

        records = []
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

        for metrics_set in metrics:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
 
            for field, value in metrics_set.common_stats.items():
                timestream_record = AWSTimestreamRecord(
                    'stage_metrics',
                    metrics_set.name,
                    metrics_set.stage,
                    'common',
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
                TableName=self.stage_metrics_table_name,
                Records=records,
                CommonAttributes={}
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating table - Database: {self.database_name} - Table: {self.metrics_table_name} - if not exists')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.metrics_table_name,
                    RetentionProperties=self.retention_options
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created table - Database: {self.database_name} - Table: {self.metrics_table_name} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of table - Database: {self.database_name} - Table: {self.metrics_table_name} - if not exists')

        records = []
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

        for metrics_set in metrics:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            metrics_groups = {**metrics_set.groups, **metrics_set.custom_metrics}

            for group_name, group in metrics_groups.items():

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {group_name}')
            
                metric_result = {**group.stats, **group.custom}

                for field, value in metric_result.items():
                    timestream_record = AWSTimestreamRecord(
                        'metric',
                        metrics_set.name,
                        metrics_set.stage,
                        group_name,
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Skipping Custom Metrics submission')
        
    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        try:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating table - Database: {self.database_name} - Table: {self.errors_table_name} - if not exists')

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.create_table,
                    DatabaseName=self.database_name,
                    TableName=self.errors_table_name,
                    RetentionProperties=self.retention_options
                )
            )

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created table - Database: {self.database_name} - Table: {self.errors_table_name} - if not exists')

        except Exception:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Skipping creation of table - Database: {self.database_name} - Table: {self.errors_table_name} - if not exists')

        error_records = []
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Errors Metrics - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')

        for metrics_set in metrics_sets:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                timestream_record = AWSTimestreamErrorRecord(
                    metrics_set.name,
                    metrics_set.stage,
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics - Database: {self.database_name} - Table: {self.events_table_name} - if not exists')


    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')