from datetime import datetime
import uuid
from typing import List

from numpy import float32, float64, int16, int32, int64
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    import sqlalchemy
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.sql import func
    from sqlalchemy.dialects.postgresql import UUID
    from hedra.reporting.types.postgres.postgres import Postgres
    from .timescaledb_config import TimescaleDBConfig
    has_connector=True

except Exception:
    sqlalchemy = None
    UUID = None
    from hedra.reporting.types.empty import Empty as Postgres
    CreateTable = None
    TimescaleDBConfig = None
    has_connector = False


class TimescaleDB(Postgres):

    def __init__(self, config: TimescaleDBConfig) -> None:
        super().__init__(config)

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Initiating transaction')
        
            for event in events:

                if self._events_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events table - {self.events_table_name} - if not exists')

                    events_table = sqlalchemy.Table(
                        self.events_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('request_time', sqlalchemy.Float),
                        sqlalchemy.Column('succeeded', sqlalchemy.Boolean),
                        sqlalchemy.Column('time', sqlalchemy.TIMESTAMP(timezone=False), nullable=False, default=datetime.now())
                    )

                    await self._connection.execute(CreateTable(events_table, if_not_exists=True))
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.events_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(f"CREATE INDEX ON {self.events_table_name} (name, time DESC);")

                    self._events_table = events_table
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Events table - {self.events_table_name}')
                
                record = event.record
                record['request_time'] = record['time']
                del record['time']

                await self._connection.execute(self._events_table.insert(values={
                    **record,
                    'time': datetime.now().timestamp()
                }))
                
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Table - {self.events_table_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}')
        
        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction')

            if self._stage_metrics_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists')

                stage_metrics_table = sqlalchemy.Table(
                    self.stage_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', UUID(as_uuid=True), default=uuid.uuid4),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('total', sqlalchemy.BIGINT),
                    sqlalchemy.Column('succeeded', sqlalchemy.BIGINT),
                    sqlalchemy.Column('failed', sqlalchemy.BIGINT),
                    sqlalchemy.Column('actions_per_second', sqlalchemy.FLOAT),
                    sqlalchemy.Column('time', sqlalchemy.TIMESTAMP(timezone=False), nullable=False, default=datetime.now())
                )

                await self._connection.execute(CreateTable(stage_metrics_table, if_not_exists=True))
                await self._connection.execute(
                    f"SELECT create_hypertable('{self.stage_metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                )

                await self._connection.execute(f"CREATE INDEX ON {self.stage_metrics_table_name} (name, time DESC);")

                self._stage_metrics_table = stage_metrics_table

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
                await self._connection.execute(
                    self._stage_metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Table - {self.shared_metrics_table_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Initiating transaction')

            for metrics_set in metrics:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._metrics_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - if not exists')

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), default=uuid.uuid4),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('group', sqlalchemy.TEXT),
                        sqlalchemy.Column('median', sqlalchemy.FLOAT),
                        sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                        sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                        sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                        sqlalchemy.Column('maximum', sqlalchemy.FLOAT),
                        sqlalchemy.Column('time', sqlalchemy.TIMESTAMP(timezone=False), nullable=False, default=datetime.now())
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(
                            sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                        )

                    for custom_field_name, sql_alchemy_type in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)

                    await self._connection.execute(CreateTable(metrics_table, if_not_exists=True))
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(f"CREATE INDEX ON {self.metrics_table_name} (name, time DESC);")

                    self._metrics_table = metrics_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}')

                for group_name, group in metrics_set.groups.items():
                    await self._connection.execute(self._metrics_table.insert(values={
                        **group.record,
                        'group': group_name
                    }))

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Table - {self.metrics_table_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Initiating transaction')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
                
                for custom_group_name, group in metrics_set.custom_metrics.items():
                    custom_table_name = f'{custom_group_name}_metrics'
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to table - {custom_group_name}')

                    if self._custom_metrics_tables.get(custom_table_name) is None:
                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics table - {custom_group_name} - if not exists')

                        custom_metrics_table = sqlalchemy.Table(
                            custom_table_name,
                            self.metadata,
                            sqlalchemy.Column('id', UUID(as_uuid=True), default=uuid.uuid4),
                            sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column('group', sqlalchemy.TEXT),
                            sqlalchemy.Column('time', sqlalchemy.TIMESTAMP(timezone=False), nullable=False, default=datetime.now())
                        )

                        for field, value in group.items():

                            if isinstance(value, (int, int16, int32, int64)):
                                custom_metrics_table.append_column(
                                    sqlalchemy.Column(field, sqlalchemy.INTEGER)
                                )

                            elif isinstance(value, (float, float32, float64)):
                                custom_metrics_table.append_column(
                                    sqlalchemy.Column(field, sqlalchemy.FLOAT)
                                )


                        await self._connection.execute(CreateTable(custom_metrics_table, if_not_exists=True))
                        await self._connection.execute(
                            f"SELECT create_hypertable('{custom_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                        )

                        await self._connection.execute(f"CREATE INDEX ON {custom_table_name} (name, time DESC);")

                        self._custom_metrics_tables[custom_group_name] = custom_metrics_table

                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Custom Metrics table - {custom_group_name}')

                    await self._connection.execute(
                        self._custom_metrics_tables[custom_table_name].insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to table - {custom_group_name}')

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Transaction committed')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Initiating transaction')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._errors_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Metrics table - {self.errors_table_name} - if not exists')

                    errors_table = sqlalchemy.Table(
                        self.errors_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), default=uuid.uuid4),
                        sqlalchemy.Column('metric_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('metrics_stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_count', sqlalchemy.BIGINT),
                        sqlalchemy.Column('time', sqlalchemy.TIMESTAMP(timezone=False), nullable=False, default=datetime.now())
                    )    

                    
                    await self._connection.execute(CreateTable(errors_table, if_not_exists=True))
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.errors_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(f"CREATE INDEX ON {self.errors_table_name}_errors (name, time DESC);")

                    self._errors_table = errors_table    

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}')

                for error in metrics_set.errors:
                    await self._connection.execute(self._metrics_table.insert(values={
                        'metric_name': metrics_set.name,
                        'metrics_stage': metrics_set.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    }))
            
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Table - {self.errors_table_name}')
                
    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to {self.sql_type} at - {self.host}')

        await self._connection.close()
        self._engine.terminate()
        await self._engine.wait_closed()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to {self.sql_type} at - {self.host}')

