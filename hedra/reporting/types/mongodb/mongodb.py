from collections import defaultdict
from typing import List
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
        self.stage_metrics_collection = 'stage_metrics'
        self.errors_collection = 'stage_errors'

        self.connection: AsyncIOMotorClient = None
        self.database = None

    async def connect(self):
        if self.username and self.password:
            connection_string = f'mongodb://{self.username}:{self.password}@{self.host}/{self.database_name}'
        
        else:
            connection_string = f'mongodb://{self.host}/{self.database_name}'

        self.connection = AsyncIOMotorClient(connection_string)
        self.database = self.connection[self.database_name]

    async def submit_events(self, events: List[BaseEvent]): 
        await self.database[self.events_collection].insert_many(
            [event.record for event in events]
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        await self.database[self.stage_metrics_collection].insert_many([
            {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            } for metrics_set in metrics_sets
        ])

    async def submit_metrics(self, metrics: List[MetricsSet]):

        records = []
        for metrics_set in metrics:
            
            for group_name, group in metrics_set.groups.items():
                records.append({
                    'group': group_name,
                    **group.record
                })

        await self.database[self.metrics_collection].insert_many(records)

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        records = defaultdict(list)
        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():
                records[custom_group_name].append({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
                })

        for group_name, records in records.items():
            metrics_collection_name = f'{group_name}_metrics'
            await self.database[metrics_collection_name].insert_many(records)

    async def submit_errors(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:
            await self.database[self.errors_collection].insert_many([
                {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                } for error in metrics_set.errors
            ])

    async def close(self):
        pass