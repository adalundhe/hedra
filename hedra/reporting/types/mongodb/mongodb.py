import uuid
from collections import defaultdict
from typing import List
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    from motor.motor_asyncio import AsyncIOMotorClient
    from .mongodb_config import MongoDBConfig
    has_connector = True

except Exception:
    AsyncIOMotorClient = None
    MongoDBConfig = None
    has_connector = False



class MongoDB:

    def __init__(self, config: MongoDBConfig) -> None:
        self.host = config.host
        self.username = config.username
        self.password = config.password
        self.database_name = config.database
        self.events_collection = config.events_collection
        self.metrics_collection = config.metrics_collection
        self.shared_metrics_collection = f'{self.metrics_collection}_common'
        self.errors_collection = f'{self.metrics_collection}_errors'

        self.connection: AsyncIOMotorClient = None
        self.database = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to MongoDB instance at - {self.host} - Database: {self.database_name}')

        if self.username and self.password:
            connection_string = f'mongodb://{self.username}:{self.password}@{self.host}/{self.database_name}'
        
        else:
            connection_string = f'mongodb://{self.host}/{self.database_name}'

        self.connection = AsyncIOMotorClient(connection_string)
        self.database = self.connection[self.database_name]

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to MongoDB instance at - {self.host} - Database: {self.database_name}')

    async def submit_events(self, events: List[BaseEvent]): 
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Collection - {self.events_collection}')
        await self.database[self.events_collection].insert_many(
            [event.record for event in events]
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Collection - {self.events_collection}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Bucket - {self.shared_metrics_collection}')
        await self.database[self.shared_metrics_collection].insert_many([
            {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            } for metrics_set in metrics_sets
        ])

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Bucket - {self.shared_metrics_collection}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Bucket - {self.metrics_collection}')

        records = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            for group_name, group in metrics_set.groups.items():
                records.append({
                    'group': group_name,
                    **group.record
                })

        await self.database[self.metrics_collection].insert_many(records)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Bucket - {self.metrics_collection}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        records = defaultdict(list)
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            for custom_group_name, group in metrics_set.custom_metrics.items():
                records[custom_group_name].append({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
                })

        for group_name, records in records.items():
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Bucket - {group_name}')

            metrics_collection_name = f'{group_name}_metrics'
            await self.database[metrics_collection_name].insert_many(records)

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Bucket - {group_name}')

    async def submit_errors(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Bucket - {self.errors_collection}')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            await self.database[self.errors_collection].insert_many([
                {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                } for error in metrics_set.errors
            ])

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Bucket - {self.errors_collection}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')