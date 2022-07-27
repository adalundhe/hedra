import uuid
from typing import Any, List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    from azure.cosmos.aio import CosmosClient
    from azure.cosmos import PartitionKey
    from .cosmosdb_config import CosmosDBConfig
    has_connector = True
except ImportError:
    CosmosClient = None
    PartitionKey = None
    CosmosDBConfig = None
    has_connector = False


class CosmosDB:

    def __init__(self, config: CosmosDBConfig) -> None:
        self.account_uri = config.account_uri
        self.account_key = config.account_key
        self.database_name = config.database
        self.events_container_name = config.events_container
        self.metrics_container_name = config.metrics_container
        self.events_partition_key = config.events_partition_key
        self.metrics_partition_key = config.metrics_partition_key
        self.analytics_ttl = config.analytics_ttl

        self.events_container = None
        self.metrics_container = None
        self.client = None
        self.database = None

    async def connect(self):
        self.client = CosmosClient(
            self.account_uri,
            credential=self.account_key
        )

        self.database = await self.client.create_database_if_not_exists(self.database_name)

    async def submit_events(self, events: List[BaseEvent]):

        self.events_container = await self.database.create_container_if_not_exists(
            self.events_container_name,
            PartitionKey(f'/{self.events_partition_key}')
        )

        for event in events:
            await self.events_container.upsert_item({
                'id': str(uuid.uuid4()),
                **event.record
            })
        
    async def submit_metrics(self, metrics: List[Metric]):

        self.metrics_container = await self.database.create_container_if_not_exists(
            self.metrics_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )

        for metric in metrics:
            await self.metrics_container.upsert_item({
                'id': str(uuid.uuid4()),
                **metric.record
            })

    async def close(self):
        await self.client.close()
        