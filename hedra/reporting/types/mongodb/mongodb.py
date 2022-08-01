from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


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
        self.group_metrics_collection = f'{self.metrics_collection}_group_metrics'
        self.errors_collection = f'{self.metrics_collection}_errors'

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

    async def submit_common(self, metrics_groups: List[MetricsGroup]):
        await self.database[self.group_metrics_collection].insert_many([
            {
                'name': metrics_group.name,
                'stage': metrics_group.stage,
                **metrics_group.common_stats
            } for metrics_group in metrics_groups
        ])

    async def submit_metrics(self, metrics: List[MetricsGroup]):
        await self.database[self.metrics_collection].insert_many(
            [{
                'name': metrics_group.name,
                'stage': metrics_group.stage,
                **metrics_group.common_stats,
                'timings': {
                    group_name: group.record for group_name, group in metrics_group.groups.items()
                }
            } for metrics_group in metrics]
        )

    async def submit_errors(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:
            await self.database[self.errors_collection].insert_many([
                {
                    'name': metrics_group.name,
                    'stage': metrics_group.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                } for error in metrics_group.errors
            ])

    async def close(self):
        pass