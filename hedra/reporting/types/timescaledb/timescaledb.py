import uuid
from typing import Any, List


try:
    import sqlalchemy
    from sqlalchemy.dialects.postgresql import UUID
    from hedra.reporting.types.postgres.postgres import Postgres
    has_connector=True

except ImportError:
    has_connector = False


class TimescaleDB(Postgres):

    def __init__(self, config: Any) -> None:
        super().__init__(config)

    async def submit_events(self, events: List[Any]):
        for event in events:

            if self._events_table is None:
                events_table = sqlalchemy.Table(
                    self.events_table,
                    self.metadata,
                    sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('request_time', sqlalchemy.Float),
                    sqlalchemy.Column('succeeded', sqlalchemy.Boolean),
                    sqlalchemy.Column('time', sqlalchemy.TIMESTAMP, nullable=False)
                )

                await events_table.create(self._connection, checkfirst=True)
                await self._connection.execute(f'SELECT create_hypertable("{self.events_table}", "time")')
                self._events_table = events_table
            
            await self._connection.execute(
                self._events_table.insert(**event.record)
            )

    async def submit_metrics(self, metrics: List[Any]):
        for metric in metrics:

            if self._metrics_table is None:

                metrics_table = sqlalchemy.Table(
                    self.metrics_table,
                    self.metadata,
                    sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('total', sqlalchemy.BIGINT),
                    sqlalchemy.Column('succeeded', sqlalchemy.BIGINT),
                    sqlalchemy.Column('failed', sqlalchemy.BIGINT),
                    sqlalchemy.Column('median', sqlalchemy.FLOAT),
                    sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                    sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                    sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                    sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                    sqlalchemy.Column('maximum', sqlalchemy.FLOAT),
                    sqlalchemy.Column('time', sqlalchemy.TIMESTAMP, nullable=False)
                )

                for quantile in metric.quantiles:
                    metrics_table.append_column(
                        sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                    )

                for custom_field_name, sql_alchemy_type in self.custom_fields:
                    metrics_table.append_column(custom_field_name, sql_alchemy_type)    

                await metrics_table.create(self._connection, checkfirst=True)
                await self._connection.execute(f'SELECT create_hypertable("{self.metrics_table}", "time")')
                self._metrics_table = metrics_table

            await self._connection.execute(
                self._metrics_table.insert(**metric.record)
            )
            
        
    async def close(self):
        await self._engine.close()

