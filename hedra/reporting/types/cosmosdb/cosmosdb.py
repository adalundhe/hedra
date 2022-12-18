import uuid
from typing import List
from hedra.logging import HedraLogger
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
        self.shared_metrics_container_name = f'{self.metrics_container_name}_metrics'
        self.errors_container_name = f'{self.metrics_container}_errors'
        self.events_partition_key = config.events_partition_key
        self.metrics_partition_key = config.metrics_partition_key
        self.custom_metrics_containers = {}

        self.analytics_ttl = config.analytics_ttl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.events_container = None
        self.metrics_container = None
        self.shared_metrics_container = None
        self.errors_container = None
        self.client = None
        self.database = None

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to CosmosDB')

        self.client = CosmosClient(
            self.account_uri,
            credential=self.account_key
        )


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to CosmosDB')
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Database - {self.database_name} - if not exists')
        self.database = await self.client.create_database_if_not_exists(self.database_name)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Database - {self.database_name}')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Events container - {self.events_container_name} with Partition Key /{self.events_container_name} if not exists')
        self.events_container = await self.database.create_container_if_not_exists(
            self.events_container_name,
            PartitionKey(f'/{self.events_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Events container - {self.events_container_name} with Partition Key /{self.events_container_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to container - {self.events_container_name} with Partition Key /{self.events_container_name}')
        for event in events:
            await self.events_container.upsert_item({
                'id': str(uuid.uuid4()),
                **event.record
            })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to container - {self.events_container_name} with Partition Key /{self.events_container_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Shared Metrics container - {self.shared_metrics_container_name} with Partition Key /{self.shared_metrics_container_name} if not exists')
        self.shared_metrics_container = await self.database.create_container_if_not_exists(
            self.shared_metrics_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Shared Metrics container - {self.shared_metrics_container_name} with Partition Key /{self.shared_metrics_container_name}')


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to container - {self.shared_metrics_container_name} with Partition Key /{self.shared_metrics_container_name}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            await self.shared_metrics_container.upsert_item({
                'id': str(uuid.uuid4()),
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to container - {self.shared_metrics_container_name} with Partition Key /{self.shared_metrics_container_name}')
        
    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Metrics container - {self.metrics_container_name} with Partition Key /{self.metrics_container_name} if not exists')
        self.metrics_container = await self.database.create_container_if_not_exists(
            self.metrics_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Metrics container - {self.metrics_container_name} with Partition Key /{self.metrics_container_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_container_name}')
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}')

                await self.metrics_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'group': group_name,
                    **group.record
                })
            
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_container_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Group - {custom_group_name}')

                custom_metrics_container_name = f'{custom_group_name}_metrics'
                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Custom Metrics container - {custom_metrics_container_name} with Partition Key /{custom_metrics_container_name} if not exists')

                custom_container = await self.database.create_container_if_not_exists(
                    custom_metrics_container_name,
                    PartitionKey(f'/{self.metrics_partition_key}')
                )

                self.custom_metrics_containers[custom_metrics_container_name] = custom_container


                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Custom Metrics container - {custom_metrics_container_name} with Partition Key /{custom_metrics_container_name}')

                await custom_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
                })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_container_name}')


    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Error Metrics container - {self.errors_container_name} with Partition Key /{self.errors_container_name} if not exists')
        self.errors_container = await self.database.create_container_if_not_exists(
            self.errors_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Error Metrics container - {self.errors_container_name} with Partition Key /{self.errors_container_name}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to container - {self.errors_container_name} with Partition Key /{self.errors_container_name}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                await self.metrics_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'error_message': error.get('message'),
                    'error_count': error.get('count')
                })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to container - {self.errors_container_name} with Partition Key /{self.errors_container_name}')

    async def close(self):

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection to CosmosDB')

        await self.client.close()
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection to CosmosDB')