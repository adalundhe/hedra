import uuid
from typing import Any, List


try:
    from azure.cosmos.aio import CosmosClient
    from azure.cosmos import PartitionKey
    has_connector = True
except ImportError:
    has_connector = False


class CosmosDB:

    def __init__(self, config: Any) -> None:
        self.account_uri = config.account_uri
        self.account_key = config.account_key
        self.database_name = config.database
        self.events_container_name = config.events_container
        self.metrics_container_name = config.metrics_container
        self.analytics_ttl = config.analytics_ttl or 0
        self.events_partition = config.events_partition or f'/{self.events_container_name}'
        self.metrics_partition = config.metrics_partition or f'/{self.metrics_container_name}'

        self.events_container = None
        self.metrics_container = None
        self.client = None
        self.database = None

    async def connect(self):
        self.client = CosmosClient(
            self.account_uri,
            credential=self.account_key
        )

        self.database = self.client.get_database_client(self.database_name)

    async def submit_events(self, events: List[Any]):

        self.events_container = await self.database.create_container_if_not_exists(
            id=self.events_container_name,
            partition_key=PartitionKey(self.events_partition),
            analytical_storage_ttl=self.analytics_ttl
        )

        for event in events:
            await self.events_container.upsert_item({
                'id': uuid.uuid4(),
                **event.record
            })
        
    async def submit_metrics(self, metrics: List[Any]):

        self.metrics_container = await self.database.create_container_if_not_exists(
            id=self.metrics_container_name,
            partition_key=PartitionKey(self.metrics_partition),
            analytical_storage_ttl=self.analytics_ttl
        )

        for metric in metrics:
            await self.metrics_container.upsert_item({
                'id': uuid.uuid4(),
                **metric.record
            })

    async def close(self):
        pass
        