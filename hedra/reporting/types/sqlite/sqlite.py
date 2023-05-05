import uuid
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import (
    MetricsSet,
    MetricType
)
from .sqlite_config import SQLiteConfig


try:
    import sqlalchemy
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.ext.asyncio import create_async_engine
    
    has_connector = True

except Exception:
    ASYNCIO_STRATEGY = None
    sqlalchemy = None
    CreateTable = None
    OperationalError = None
    has_connector = False



class SQLite:

    def __init__(self, config: SQLiteConfig) -> None:
        self.path = f'sqlite+aiosqlite:///{config.path}'
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table


        self.experiments_table_name = config.experiments_table
        self.streams_table_name = config.streams_table
        self.variants_table_name = f'{config.experiments_table}_variants'
        self.mutations_table_name = f'{config.experiments_table}_mutations'

        self.shared_metrics_table_name = f'{config.metrics_table}_shared'
        self.errors_table_name = f'{config.metrics_table}_errors'
        self.custom_metrics_table_name = f'{config.metrics_table}_custom'
        self.metadata = sqlalchemy.MetaData()

        self.database = None
        self._engine = None
        self._connection = None

        self._events_table = None
        self._metrics_table = None
        self._streams_table = None

        self._experiments_table = None
        self._variants_table = None
        self._mutations_table = None

        self._shared_metrics_table = None
        self._custom_metrics_table = None
        self._errors_table = None

        self.metric_types_map = {
            MetricType.COUNT: lambda field_name: sqlalchemy.Column(
                field_name, 
                sqlalchemy.BIGINT
            ),
            MetricType.DISTRIBUTION: lambda field_name: sqlalchemy.Column(
                field_name, 
                sqlalchemy.FLOAT
            ),
            MetricType.SAMPLE: lambda field_name: sqlalchemy.Column(
                field_name, 
                sqlalchemy.FLOAT
            ),
            MetricType.RATE: lambda field_name: sqlalchemy.Column(
                field_name, 
                sqlalchemy.FLOAT
            ),
        }

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()


    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to SQLite at - {self.path} - Database: {self.database}')
        self._engine = create_async_engine(self.path)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to SQLite at - {self.path} - Database: {self.database}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name} - Initiating transaction')
        
            for stage_name, stream in stream_metrics.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams - {stage_name}:{stream.stream_set_id}')

                if self._streams_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Streams table - {self.streams_table_name} - if not exists')

                    stream_table = sqlalchemy.Table(
                        self.streams_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('group', sqlalchemy.TEXT()),
                        sqlalchemy.Column('median', sqlalchemy.FLOAT),
                        sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                        sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                        sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                        sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
                    )

                    for quantile in stream.quantiles:
                        stream_table.append_column(
                            sqlalchemy.Column(f'quantile_{quantile}th', sqlalchemy.FLOAT)
                        )

                    await self._connection.execute(
                        CreateTable(
                            stream_table, 
                            if_not_exists=True
                        )
                    )
                
                    self._streams_table = stream_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Streams table - {self.streams_table_name}')

                for group_name, group in stream.grouped.items():
                    await self._connection.execute(
                        self._streams_table.insert(values={
                            'name': f'{stage_name}_streams',
                            'stage': stage_name,
                            'group': group_name,
                            **group
                        })
                    )
                    
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Table - {self.streams_table_name}')

    async def submit_experiments(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name}')
        
        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name} - Initiating transaction')
        
            for experiment in experiments_metrics.experiment_summaries:

                if self._experiments_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Experiments table - {self.experiments_table_name} - if not exists')

                    experiments_table = sqlalchemy.Table(
                        self.experiments_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column('experiment_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('experiment_randomized', sqlalchemy.BOOLEAN),
                        sqlalchemy.Column('experiment_completed', sqlalchemy.BIGINT),
                        sqlalchemy.Column('experiment_succeeded', sqlalchemy.BIGINT),
                        sqlalchemy.Column('experiment_failed', sqlalchemy.BIGINT),
                        sqlalchemy.Column('experiment_median_aps', sqlalchemy.FLOAT),
                    )

                    await self._connection.execute(CreateTable(experiments_table, if_not_exists=True))

                    self._experiments_table = experiments_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Experiments table - {self.experiments_table_name}')
                
                await self._connection.execute(self._experiments_table.insert().values(**experiment.record))
                    
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Table - {self.experiments_table_name}')

    async def submit_variants(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name}')
        
        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name} - Initiating transaction')
        
            for variant in experiments_metrics.variant_summaries:

                if self._variants_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Variants table - {self.variants_table_name} - if not exists')

                    variants_table = sqlalchemy.Table(
                        self.variants_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column('variant_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('variant_experiment', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('variant_weight', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variant_distribution', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('variant_distribution_interval', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variant_completed', sqlalchemy.BIGINT),
                        sqlalchemy.Column('variant_succeeded', sqlalchemy.BIGINT),
                        sqlalchemy.Column('variant_failed', sqlalchemy.BIGINT),
                        sqlalchemy.Column('variant_actions_per_second', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variant_ratio_completed', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variant_ratio_succeeded', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variant_ratio_failed', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variant_ratio_aps', sqlalchemy.FLOAT),
                    )

                    await self._connection.execute(CreateTable(variants_table, if_not_exists=True))

                    self._variants_table = variants_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Variants table - {self.variants_table_name}')
                
                await self._connection.execute(self._variants_table.insert().values(**variant.record))
                    
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Table - {self.variants_table_name}')

    async def submit_mutations(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name}')
        
        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name} - Initiating transaction')
        
            for mutation in experiments_metrics.mutation_summaries:

                if self._mutations_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Mutations table - {self.mutations_table_name} - if not exists')

                    mutations_table = sqlalchemy.Table(
                        self.mutations_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                        sqlalchemy.Column('mutation_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('mutation_experiment_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('mutation_variant_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('mutation_chance', sqlalchemy.FLOAT),
                        sqlalchemy.Column('mutation_targets', sqlalchemy.VARCHAR(8192)),
                        sqlalchemy.Column('mutation_type', sqlalchemy.VARCHAR(255)),
                    )

                    await self._connection.execute(CreateTable(mutations_table, if_not_exists=True))

                    self._mutations_table = mutations_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Mutations table - {self.mutations_table_name}')
                
                await self._connection.execute(self._mutations_table.insert().values(**mutation.record))
                    
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Table - {self.mutations_table_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Initiating transaction')

            for event in events:

                if self._events_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events table - {self.events_table_name} - if not exists')

                    events_table = sqlalchemy.Table(
                        self.events_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True,),
                        sqlalchemy.Column('name', sqlalchemy.TEXT),
                        sqlalchemy.Column('stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('time', sqlalchemy.REAL),
                        sqlalchemy.Column('succeeded', sqlalchemy.INTEGER)
                    )

                    
                    await connection.execute(CreateTable(events_table, if_not_exists=True))
                    self._events_table = events_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Events table - {self.events_table_name}')
            
                await connection.execute(self._events_table.insert(values=event.record))
            
            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Table - {self.events_table_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction')

            if self._shared_metrics_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists')

                stage_metrics_table = sqlalchemy.Table(
                    self.shared_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.TEXT),
                    sqlalchemy.Column('stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('group', sqlalchemy.TEXT),
                    sqlalchemy.Column('total', sqlalchemy.INTEGER),
                    sqlalchemy.Column('succeeded', sqlalchemy.INTEGER),
                    sqlalchemy.Column('failed', sqlalchemy.INTEGER),
                    sqlalchemy.Column('actions_per_second', sqlalchemy.REAL)
                )

                await connection.execute(CreateTable(stage_metrics_table, if_not_exists=True))
                self._shared_metrics_table = stage_metrics_table

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}')

            for metrics_set in metrics_sets:
                await connection.execute(
                    self._shared_metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Table - {self.shared_metrics_table_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Initiating transaction')

            for metrics_set in metrics:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._metrics_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - if not exists')

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                        sqlalchemy.Column('name', sqlalchemy.TEXT),
                        sqlalchemy.Column('stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('group', sqlalchemy.TEXT),
                        sqlalchemy.Column('median', sqlalchemy.REAL),
                        sqlalchemy.Column('mean', sqlalchemy.REAL),
                        sqlalchemy.Column('variance', sqlalchemy.REAL),
                        sqlalchemy.Column('stdev', sqlalchemy.REAL),
                        sqlalchemy.Column('minimum', sqlalchemy.REAL),
                        sqlalchemy.Column('maximum', sqlalchemy.REAL)
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(
                            sqlalchemy.Column(f'{quantile}', sqlalchemy.REAL)
                        )

                    for custom_field_name, sql_alchemy_type in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)   
                    
                    await connection.execute(CreateTable(metrics_table, if_not_exists=True))
                    self._metrics_table = metrics_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}')

                for group_name, group in metrics_set.groups.items():
                    await connection.execute(self._metrics_table.insert(values={
                        **group.record,
                        'group': group_name
                    }))

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Table - {self.metrics_table_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Initiating transaction')

            if self._custom_metrics_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics table - {self.custom_metrics_table_name} - if not exists')

                custom_metrics_table = sqlalchemy.Table(
                    self.custom_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.TEXT),
                    sqlalchemy.Column('stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('group', sqlalchemy.TEXT),
                )

                for metrics_set in metrics_sets:

                    for custom_metric_name, custom_metric in metrics_set.custom_metrics.items():

                        custom_metrics_table.append_column(
                            self.metric_types_map.get(
                                custom_metric.metric_type,
                                lambda field_name: sqlalchemy.Column(
                                    field_name, 
                                    sqlalchemy.FLOAT
                                )
                            )(custom_metric_name)
                        )

                await connection.execute(CreateTable(custom_metrics_table, if_not_exists=True))
                self._custom_metrics_table = custom_metrics_table

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Custom Metrics table - {self.custom_metrics_table_name}')

            for metrics_set in metrics_sets:
                
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
                    
                await connection.execute(
                    self._custom_metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'custom',
                        **{
                            custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                        }
                    })
                )

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitte Cudstom Metrics - Transaction committed')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Initiating transaction')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._errors_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Metrics table - {self.errors_table_name} - if not exists')

                    metrics_errors_table = sqlalchemy.Table(
                        self.errors_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                        sqlalchemy.Column('name', sqlalchemy.TEXT),
                        sqlalchemy.Column('stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_count', sqlalchemy.INTEGER)
                    ) 

                    await connection.execute(CreateTable(metrics_errors_table, if_not_exists=True))

                    self._errors_table = metrics_errors_table
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}')

                for error in metrics_set.errors:
                    await connection.execute(
                        self._errors_table.insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        })
                    )

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Table - {self.errors_table_name}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
