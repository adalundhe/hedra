import uuid
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    from azure.cosmos.aio import CosmosClient
    from azure.cosmos import PartitionKey
    from .cosmosdb_config import CosmosDBConfig
    has_connector = True
except Exception:
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
        self.stage_metrics_container_name = 'stage_metrics'
        self.errors_container_name = 'stage_errors'
        self.custom_metrics_containers = {}

        self.analytics_ttl = config.analytics_ttl

        self.events_container = None
        self.metrics_container = None
        self.group_metrics_container = None
        self.errors_container = None
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
            PartitionKey(f'/{self.events_container_name}')
        )

        for event in events:
            await self.events_container.upsert_item({
                'id': str(uuid.uuid4()),
                **event.record
            })

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        self.group_metrics_container = await self.database.create_container_if_not_exists(
            self.stage_metrics_container_name,
            PartitionKey(f'/{self.stage_metrics_container_name}')
        )

        for metrics_set in metrics_sets:
            await self.group_metrics_container.upsert_item({
                'id': str(uuid.uuid4()),
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            })
        
    async def submit_metrics(self, metrics: List[MetricsSet]):

        self.metrics_container = await self.database.create_container_if_not_exists(
            self.metrics_container_name,
            PartitionKey(f'/{self.metrics_container_name}')
        )

        for metrics_set in metrics:
            for group_name, group in metrics_set.groups.items():
                await self.metrics_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'group': group_name,
                    **group.record
                })

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_metrics_container_name = f'{custom_group_name}_metrics'

                custom_container = await self.database.create_container_if_not_exists(
                    custom_metrics_container_name,
                    PartitionKey(f'/{custom_group_name}_metrics')
                )

                self.custom_metrics_containers[custom_metrics_container_name] = custom_container

                await custom_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
                })

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        self.errors_container = await self.database.create_container_if_not_exists(
            self.errors_container_name,
            PartitionKey(f'/{self.errors_container_name}')
        )

        for metrics_set in metrics_sets:
            for error in metrics_set.errors:
                await self.metrics_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                })

    async def close(self):
        await self.client.close()
        