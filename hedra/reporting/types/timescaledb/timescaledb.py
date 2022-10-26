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

        async with self._connection.begin() as transaction:
        
            for event in events:

                if self._events_table is None:
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
                
                record = event.record
                record['request_time'] = record['time']
                del record['time']

                await self._connection.execute(self._events_table.insert(values={
                    **record,
                    'time': datetime.now().timestamp()
                }))
                
            await transaction.commit()

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        
        async with self._connection.begin() as transaction:

            if self._stage_metrics_table is None:

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

            for metrics_set in metrics_sets:
                await self._connection.execute(
                    self._stage_metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

            await transaction.commit()

    async def submit_metrics(self, metrics: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            for metrics_set in metrics:

                if self._metrics_table is None:

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

                for group_name, group in metrics_set.groups.items():
                    await self._connection.execute(self._metrics_table.insert(values={
                        **group.record,
                        'group': group_name
                    }))

            await transaction.commit()

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            for metrics_set in metrics_sets:
                
                for custom_group_name, group in metrics_set.custom_metrics.items():
                    custom_table_name = f'{custom_group_name}_metrics'

                    if self._custom_metrics_tables.get(custom_table_name) is None:

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

                    await self._connection.execute(
                        self._custom_metrics_tables[custom_table_name].insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

            await transaction.commit()

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            for metrics_set in metrics_sets:

                if self._errors_table is None:
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


                for error in metrics_set.errors:
                    await self._connection.execute(self._metrics_table.insert(values={
                        'metric_name': metrics_set.name,
                        'metrics_stage': metrics_set.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    }))
            
            await transaction.commit()
                
        
    async def close(self):
        await self._connection.close()
        self._engine.terminate()
        await self._engine.wait_closed()


