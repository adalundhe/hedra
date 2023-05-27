import asyncio
import uuid
import psutil
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import (
    MetricsSet,
    MetricType
)
from hedra.reporting.system.system_metrics_set import (
    SystemMetricsSet,
    SessionMetricsCollection,
    SystemMetricsCollection
)
from .snowflake_config import SnowflakeConfig


try:
    import sqlalchemy
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    from sqlalchemy.schema import CreateTable
    
    has_connector = True

except Exception:
    snowflake = None
    has_connector = False

class Snowflake:

    def __init__(self, config: SnowflakeConfig) -> None:
        self.username = config.username
        self.password = config.password
        self.organization_id = config.organization_id
        self.account_id = config.account_id
        self.private_key = config.private_key
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.database_schema

        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.streams_table_name = config.streams_table

        self.experiments_table_name = config.experiments_table
        self.variants_table_name = f'{config.experiments_table}_variants'
        self.mutations_table_name = f'{config.experiments_table}_mutations'

        self.shared_metrics_table_name = f'{config.metrics_table}_shared'
        self.errors_table_name = f'{config.metrics_table}_errors'
        self.custom_metrics_table_name = f'{config.metrics_table}_custom'


        self.session_system_metrics_table_name = f'{config.system_metrics_table}_session'
        self.stage_system_metrics_table_name = f'{config.system_metrics_table}_stage'

        self.connect_timeout = config.connect_timeout
        
        self.metadata = sqlalchemy.MetaData()
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

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

        self._session_system_metrics_table = None
        self._stage_system_metrics_table = None

        self.metric_types_map = {
            MetricType.COUNT: lambda field_name: sqlalchemy.Column(
                field_name, 
                sqlalchemy.Integer
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

        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        try:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Snowflake instance at - Warehouse: {self.warehouse} - Database: {self.database} - Schema: {self.schema}')
            self._engine = await self._loop.run_in_executor(
                self._executor,
                create_engine,
                URL(
                    user=self.username,
                    password=self.password,
                    account=self.account_id,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema
                )
            
            )

            self._connection = await asyncio.wait_for(
                self._loop.run_in_executor(
                    self._executor,
                    self._engine.connect
                ),
                timeout=self.connect_timeout
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Snowflake instance at - Warehouse: {self.warehouse} - Database: {self.database} - Schema: {self.schema}')

        except asyncio.TimeoutError:
            raise Exception('Err. - Connection to Snowflake timed out - check your account id, username, and password.')

    async def submit_session_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Session System Metrics to Table - {self.session_system_metrics_table_name}')

        if self._session_system_metrics_table is None:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Session System Metrics table - {self.session_system_metrics_table_name} - if not exists')

            session_system_metrics_table = sqlalchemy.Table(
                self.session_system_metrics_table_name,
                self.metadata,
                sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('median', sqlalchemy.FLOAT),
                sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
            )

            for quantile in SystemMetricsSet.quantiles:
                session_system_metrics_table.append_column(
                    sqlalchemy.Column(f'quantile_{quantile}th', sqlalchemy.FLOAT)
                )
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                CreateTable(
                    session_system_metrics_table, 
                    if_not_exists=True
                )
            )

            self._session_system_metrics_table = session_system_metrics_table
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Session System Metrics table - {self.session_system_metrics_table_name}')

        rows: List[SessionMetricsCollection] = []
            
        for metrics_set in system_metrics_sets:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Session System Metrics - {metrics_set.system_metrics_set_id}')

            for monitor_metrics in metrics_set.session_cpu_metrics.values():
                rows.append(monitor_metrics)
                
            for  monitor_metrics in metrics_set.session_memory_metrics.values():
                rows.append(monitor_metrics)

        for metrics_set in rows:
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._session_system_metrics_table.insert(values=metrics_set.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Session System Metrics to Table - {self.stage_system_metrics_table_name}')
    
    async def submit_stage_system_metrics(self, system_metrics_sets: List[SystemMetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Stage System Metrics to Table - {self.stage_system_metrics_table_name}')

        if self._stage_system_metrics_table is None:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Stage System Metrics table - {self.stage_system_metrics_table_name} - if not exists')

            stage_system_metrics_table = sqlalchemy.Table(
                self.stage_system_metrics_table_name,
                self.metadata,
                sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('median', sqlalchemy.FLOAT),
                sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
            )

            for quantile in SystemMetricsSet.quantiles:
                stage_system_metrics_table.append_column(
                    sqlalchemy.Column(f'quantile_{quantile}th', sqlalchemy.FLOAT)
                )
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                CreateTable(
                    stage_system_metrics_table, 
                    if_not_exists=True
                )
            )

            self._stage_system_metrics_table = stage_system_metrics_table
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Stage System Metrics table - {self.stage_system_metrics_table_name}')

        rows: List[SessionMetricsCollection] = []
            
        for metrics_set in system_metrics_sets:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Stage System Metrics - {metrics_set.system_metrics_set_id}')

            cpu_metrics = metrics_set.cpu
            memory_metrics = metrics_set.memory

            for stage_name, stage_cpu_metrics in  cpu_metrics.metrics.items():

                for monitor_metrics in stage_cpu_metrics.values():
                    rows.append(monitor_metrics)

                stage_memory_metrics = memory_metrics.metrics.get(stage_name)
                for monitor_metrics in stage_memory_metrics.values():
                    rows.append(monitor_metrics)

                stage_mb_per_vu_metrics = metrics_set.mb_per_vu.get(stage_name)
                
                if stage_mb_per_vu_metrics:
                    rows.append(stage_mb_per_vu_metrics)

        for metrics_set in rows:
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._stage_system_metrics_table.insert(values=metrics_set.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Stage System Metrics to Table - {self.stage_system_metrics_table_name}')

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Table - {self.streams_table_name}')

        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams - {stage_name}:{stream.stream_set_id}')

            if self._streams_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Streams table - {self.streams_table_name} - if not exists')

                streams_table = sqlalchemy.Table(
                    self.streams_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('median', sqlalchemy.FLOAT),
                    sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                    sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                    sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                    sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                    sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
                )

                for quantile in stream.quantiles:
                    streams_table.append_column(
                        sqlalchemy.Column(f'quantile_{quantile}th', sqlalchemy.FLOAT)
                    )
                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(
                        streams_table, 
                        if_not_exists=True
                    )
                )

                self._streams_table = streams_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Streams table - {self.streams_table_name}')

        for group_name, group in stream.grouped.items():
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._streams_table.insert(values={
                    **group,
                    'group': group_name,
                    'stage': stage_name,
                    'name': f'{stage_name}_streams'
                })
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Table - {self.streams_table_name}')

    async def submit_experiments(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Table - {self.experiments_table_name}')
    
        for experiment in experiments_metrics.experiment_summaries:

            if self._experiments_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Experiments table - {self.experiments_table_name} - if not exists')
                experiments_table = sqlalchemy.Table(
                    self.experiments_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('experiment_name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('experiment_randomized', sqlalchemy.Boolean),
                    sqlalchemy.Column('experiment_completed', sqlalchemy.BIGINT),
                    sqlalchemy.Column('experiment_succeeded', sqlalchemy.BIGINT),
                    sqlalchemy.Column('experiment_failed', sqlalchemy.BIGINT),
                    sqlalchemy.Column('experiment_median_aps', sqlalchemy.FLOAT),
                )
                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(experiments_table, if_not_exists=True)
                )

                self._experiments_table = experiments_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Experiments table - {self.experiments_table_name}')
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._experiments_table.insert(values=experiment.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Table - {self.experiments_table_name}')

    async def submit_variants(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Table - {self.variants_table_name}')
    
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
                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(variants_table, if_not_exists=True)
                )

                self._variants_table = variants_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Variants table - {self.variants_table_name}')
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._variants_table.insert(values=variant.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Table - {self.variants_table_name}')

    async def submit_mutations(self, experiments_metrics: ExperimentMetricsCollectionSet):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Table - {self.mutations_table_name}')
    
        for variant in experiments_metrics.variant_summaries:

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
                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(mutations_table, if_not_exists=True)
                )

                self._mutations_table = mutations_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Mutations table - {self.mutations_table_name}')
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._mutations_table.insert(values=variant.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Table - {self.mutations_table_name}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name}')
    
        for event in events:

            if self._events_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events table - {self.events_table_name} - if not exists')
                events_table = sqlalchemy.Table(
                    self.events_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('time', sqlalchemy.Float),
                    sqlalchemy.Column('succeeded', sqlalchemy.Boolean),
                )
                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(events_table, if_not_exists=True)
                )

                self._events_table = events_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Events table - {self.events_table_name}')
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._events_table.insert(values=event.record)
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Table - {self.events_table_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}')

        if self._shared_metrics_table is None:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists')

            shared_metrics_table = sqlalchemy.Table(
                self.shared_metrics_table_name,
                self.metadata,
                sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('group', sqlalchemy.TEXT),
                sqlalchemy.Column('total', sqlalchemy.BIGINT),
                sqlalchemy.Column('succeeded', sqlalchemy.BIGINT),
                sqlalchemy.Column('failed', sqlalchemy.BIGINT),
                sqlalchemy.Column('actions_per_second', sqlalchemy.FLOAT)
            )

            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                CreateTable(shared_metrics_table, if_not_exists=True)
            )

            self._shared_metrics_table = shared_metrics_table
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._shared_metrics_table.insert(values={
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'common',
                    **metrics_set.common_stats
                })
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Table - {self.shared_metrics_table_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name}')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            if self._metrics_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - if not exists')

                metrics_table = sqlalchemy.Table(
                    self.metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('median', sqlalchemy.FLOAT),
                    sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                    sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                    sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                    sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                    sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
                )

                for quantile in metrics_set.quantiles:
                    metrics_table.append_column(
                        sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                    )

                for custom_field_name, sql_alchemy_type in metrics_set.custom_schemas:
                    metrics_table.append_column(custom_field_name, sql_alchemy_type)  

                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(metrics_table, if_not_exists=True)
                )

                self._metrics_table = metrics_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}')

        for group_name, group in metrics_set.groups.items():
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._metrics_table.insert(values={
                    **group.record,
                    'group': group_name
                })
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Table - {self.metrics_table_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to table - {self.custom_metrics_table_name}')

        if self._custom_metrics_table is None:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics table - {self.custom_metrics_table_name} - if not exists')

            custom_metrics_table = sqlalchemy.Table(
                self.custom_metrics_table_name,
                self.metadata,
                sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
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
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                CreateTable(custom_metrics_table, if_not_exists=True)
            )

            self._custom_metrics_table = custom_metrics_table
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Custom Metrics table - {self.custom_metrics_table_name}')


        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
                
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._custom_metrics_table.insert(values={
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'custom',
                    **{
                        custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                    }
                })
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to table - {self.custom_metrics_table_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            if self._errors_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Metrics table - {self.errors_table_name} - if not exists')

                errors_table = sqlalchemy.Table(
                    self.errors_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                    sqlalchemy.Column('error_count', sqlalchemy.BIGINT)
                )

                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(errors_table, if_not_exists=True)
                )

                self._errors_table = errors_table
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}')

            for error in metrics_set.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    self._errors_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    })
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Table - {self.errors_table_name}')
        
    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection to Snowflake instance at - Warehouse: {self.warehouse} - Database: {self.database} - Schema: {self.schema}')

        await self._loop.run_in_executor(
            None,
            self._connection.close
        )

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closed session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection to Snowflake instance at - Warehouse: {self.warehouse} - Database: {self.database} - Schema: {self.schema}')