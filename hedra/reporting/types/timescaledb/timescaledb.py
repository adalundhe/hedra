from datetime import datetime
import uuid
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


try:
    import sqlalchemy
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.sql import func
    from sqlalchemy.dialects.postgresql import UUID
    from hedra.reporting.types.postgres.postgres import Postgres
    from .timescaledb_config import TimescaleDBConfig
    has_connector=True

except ImportError:
    # sqlalchemy = None
    # UUID = None
    # Postgres = None
    # CreateTable = None
    # TimescaleDBConfig = None
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

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        async with self._connection.begin() as transaction:
            for metrics_group in metrics:

                if self._metrics_table is None:

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), default=uuid.uuid4),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('timings_group', sqlalchemy.TEXT),
                        sqlalchemy.Column('total', sqlalchemy.BIGINT),
                        sqlalchemy.Column('succeeded', sqlalchemy.BIGINT),
                        sqlalchemy.Column('failed', sqlalchemy.BIGINT),
                        sqlalchemy.Column('median', sqlalchemy.FLOAT),
                        sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                        sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                        sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                        sqlalchemy.Column('maximum', sqlalchemy.FLOAT),
                        sqlalchemy.Column('time', sqlalchemy.TIMESTAMP(timezone=False), nullable=False, default=datetime.now())
                    )

                    for quantile in metrics_group.quantiles:
                        metrics_table.append_column(
                            sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                        )

                    for custom_field_name, sql_alchemy_type in metrics_group.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)

                    await self._connection.execute(CreateTable(metrics_table, if_not_exists=True))
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.metrics_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(f"CREATE INDEX ON {self.metrics_table_name} (name, time DESC);")

                    self._metrics_table = metrics_table

                for timings_group_name, timings_group in metrics_group.groups.items():
                    await self._connection.execute(self._metrics_table.insert(values={
                        **timings_group.record,
                        'timings_group': timings_group_name
                    }))

            await transaction.commit()

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        async with self._connection.begin() as transaction:
            for metrics_group in metrics_groups:

                if self._errors_table is None:
                    errors_table = sqlalchemy.Table(
                        self.errors_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), default=uuid.uuid4),
                        sqlalchemy.Column('metric_name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('metrics_stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_count', sqlalchemy.BIGINT)
                    )    

                    
                    await self._connection.execute(CreateTable(errors_table, if_not_exists=True))
                    await self._connection.execute(
                        f"SELECT create_hypertable('{self.errors_table_name}', 'time', migrate_data => true, if_not_exists => TRUE, create_default_indexes=>FALSE);"
                    )

                    await self._connection.execute(f"CREATE INDEX ON {self.errors_table_name}_errors (name, time DESC);")

                    self._errors_table = errors_table    


                for error in metrics_group.errors:
                    await self._connection.execute(self._metrics_table.insert(values={
                        'metric_name': metrics_group.name,
                        'metrics_stage': metrics_group.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    }))
            
            await transaction.commit()
                
        
    async def close(self):
        await self._connection.close()
        self._engine.terminate()
        await self._engine.wait_closed()


