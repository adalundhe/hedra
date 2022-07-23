from typing import Any, List


try:
    from motor.motor_asyncio import AsyncIOMotorClient
    has_connector = True

except Exception:
    has_connector = False



class MongoDB:

    def __init__(self, config: Any) -> None:
        self.host = config.host
        self.username = config.username
        self.password = config.password
        self.database_name = config.database

        self.connection: AsyncIOMotorClient = None
        self.database = None
        self.events_collection = config.events_collection
        self.metrics_collection = config.metrics_collection

    async def connect(self):
        if self.username and self.password:
            connection_string = f'mongodb://{self.username}:{self.password}@{self.host}/{self.database_name}'
        
        else:
            connection_string = f'mongodb://{self.host}/{self.database_name}'

        self.connection = AsyncIOMotorClient(connection_string)
        self.database = self.connection[self.database_name]

    async def submit_events(self, events: List[Any]): 
        await self.database[self.events_collection].insert_many(
            [event.record for event in events]
        )

    async def submit_metrics(self, metrics: List[Any]):
        await self.database[self.metrics_collection].insert_many(
            [metric.record for metric in metrics]
        )

    async def close(self):
        pass