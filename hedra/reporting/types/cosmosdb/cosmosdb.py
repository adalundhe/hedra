import uuid
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from .cosmosdb_config import CosmosDBConfig


try:
    from azure.cosmos.aio import CosmosClient
    from azure.cosmos import PartitionKey
    has_connector = True
except Exception:
    CosmosClient = None
    PartitionKey = None
    has_connector = False


class CosmosDB:

    def __init__(self, config: CosmosDBConfig) -> None:
        self.account_uri = config.account_uri
        self.account_key = config.account_key

        self.database_name = config.database
        self.events_container_name = config.events_container
        self.metrics_container_name = config.metrics_container
        self.streams_container_name = config.streams_container

        self.experiments_container_name = config.experiments_container
        self.variants_container_name = f'{config.experiments_container}_variants'
        self.mutations_container_name = f'{config.experiments_container}_mutations'

        self.shared_metrics_container_name = f'{self.metrics_container_name}_metrics'
        self.custom_metrics_container_name = f'{self.metrics_container_name}_custom'
        self.errors_container_name = f'{self.metrics_container}_errors'

        self.events_partition_key = config.events_partition_key
        self.metrics_partition_key = config.metrics_partition_key
        self.streams_partition_key = config.streams_partition_key

        self.experiments_partition_key = config.experiments_partition_key
        self.variants_partition_key = config.variants_partition_key
        self.mutations_partition_key = config.mutations_partition_key


        self.analytics_ttl = config.analytics_ttl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.events_container = None
        self.metrics_container = None
        self.streams_container = None

        self.experiments_container = None
        self.variants_container = None
        self.mutations_container = None

        self.shared_metrics_container = None
        self.custom_metrics_container = None
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

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Streams container - {self.streams_container_name} with Partition Key /{self.streams_partition_key} if not exists')
        self.streams_container = await self.database.create_container_if_not_exists(
            self.streams_container_name,
            PartitionKey(f'/{self.streams_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Streams container - {self.streams_container_name} with Partition Key /{self.streams_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to container - {self.streams_container_name} with Partition Key /{self.streams_partition_key}')
        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams Set - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams Group - {group_name}:{stream.stream_set_id}')

                await self.streams_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'name': f'{stage_name}_stream',
                    'stage': stage_name,
                    'group': group_name,
                    **group
                })
            
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to container - {self.streams_container_name} with Partition Key /{self.streams_partition_key}')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Experiments container - {self.experiments_container_name} with Partition Key /{self.experiments_partition_key} if not exists')
        self.experiments_container = await self.database.create_container_if_not_exists(
            self.experiments_container_name,
            PartitionKey(f'/{self.experiments_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Experiment container - {self.experiments_container_name} with Partition Key /{self.experiments_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiment to container - {self.experiments_container_name} with Partition Key /{self.experiments_partition_key}')
        for experiment in experiment_metrics.experiments:
            await self.experiments_container.upsert_item({
                'id': str(uuid.uuid4()),
                **experiment
            })
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to container - {self.experiments_container_name} with Partition Key /{self.experiments_partition_key}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Variants container - {self.variants_container_name} with Partition Key /{self.variants_partition_key} if not exists')
        self.variants_container = await self.database.create_container_if_not_exists(
            self.variants_container_name,
            PartitionKey(f'/{self.variants_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Variants container - {self.variants_container_name} with Partition Key /{self.variants_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variant to container - {self.variants_container_name} with Partition Key /{self.variants_partition_key}')
        for variant in experiment_metrics.variants:
            await self.variants_container.upsert_item({
                'id': str(uuid.uuid4()),
                **variant
            })
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to container - {self.variants_container_name} with Partition Key /{self.variants_partition_key}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Mutations container - {self.mutations_container_name} with Partition Key /{self.mutations_partition_key} if not exists')
        self.mutations_container = await self.database.create_container_if_not_exists(
            self.mutations_container_name,
            PartitionKey(f'/{self.mutations_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Mutations container - {self.mutations_container_name} with Partition Key /{self.mutations_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutation to container - {self.mutations_container_name} with Partition Key /{self.mutations_partition_key}')
        for mutation in experiment_metrics.mutations:
            await self.mutations_container.upsert_item({
                'id': str(uuid.uuid4()),
                **mutation
            })
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to container - {self.mutations_container_name} with Partition Key /{self.mutations_partition_key}')


    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Events container - {self.events_container_name} with Partition Key /{self.events_partition_key} if not exists')
        self.events_container = await self.database.create_container_if_not_exists(
            self.events_container_name,
            PartitionKey(f'/{self.events_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Events container - {self.events_container_name} with Partition Key /{self.events_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to container - {self.events_container_name} with Partition Key /{self.events_partition_key}')
        for event in events:
            await self.events_container.upsert_item({
                'id': str(uuid.uuid4()),
                **event.record
            })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to container - {self.events_container_name} with Partition Key /{self.events_partition_key}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Shared Metrics container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key} if not exists')
        self.shared_metrics_container = await self.database.create_container_if_not_exists(
            self.shared_metrics_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Shared Metrics container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key}')


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key}')
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            await self.shared_metrics_container.upsert_item({
                'id': str(uuid.uuid4()),
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'common',
                **metrics_set.common_stats
            })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to container - {self.shared_metrics_container_name} with Partition Key /{self.metrics_partition_key}')
        
    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Metrics container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key} if not exists')
        self.metrics_container = await self.database.create_container_if_not_exists(
            self.metrics_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Metrics container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}')
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Group - {group_name}:{group.metrics_group_id}')

                await self.metrics_container.upsert_item({
                    'id': str(uuid.uuid4()),
                    'group': group_name,
                    **group.record
                })
            
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):
        
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Group - Xuarom')

            custom_metrics_container_name = f'{self.custom_metrics_container_name}_metrics'
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Custom Metrics container - {custom_metrics_container_name} with Partition Key /{self.metrics_partition_key} if not exists')

            custom_container = await self.database.create_container_if_not_exists(
                custom_metrics_container_name,
                PartitionKey(f'/{self.metrics_partition_key}')
            )

            self.custom_metrics_container = custom_container


            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Custom Metrics container - {custom_metrics_container_name} with Partition Key /{self.metrics_partition_key}')

            await custom_container.upsert_item({
                'id': str(uuid.uuid4()),
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                'group': 'custom',
                **{
                    custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                }
            })

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to container - {self.metrics_container_name} with Partition Key /{self.metrics_partition_key}')


    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Error Metrics container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key} if not exists')
        self.errors_container = await self.database.create_container_if_not_exists(
            self.errors_container_name,
            PartitionKey(f'/{self.metrics_partition_key}')
        )
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Error Metrics container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key}')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key}')
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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to container - {self.errors_container_name} with Partition Key /{self.metrics_partition_key}')

    async def close(self):

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection to CosmosDB')

        await self.client.close()
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection to CosmosDB')