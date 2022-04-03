from __future__ import annotations
import uuid
from hedra.reporting.connectors.types.postgres_async_connector.postgres_async_connector import PostgresAsyncConnector
from hedra.reporting.metrics.types import PostgresMetric
from .utils.helpers import SQLHelper



class PostgresReporter:

    def __init__(self, config):
        self.format = 'postgres'
        self.session_id = uuid.uuid4()
        self.reporter_config = config
        self.connector = PostgresAsyncConnector(self.reporter_config)

        self._default_fields = [
            {
                'name': 'format',
                'type': str
            },
            {
                'name': 'session_id',
                'type': str
            }
        ]

        self.metrics_table = SQLHelper(
            table_name=self.reporter_config.get('metrics', 'metrics'),
            tags_table_name=self.reporter_config.get('metrics_tags', 'metrics_tags'),
            model=PostgresMetric,
            optional_fields=[
                {
                    'name': 'metric_uuid',
                    'type': str
                },
                *self._default_fields
            ]
        )

    @classmethod
    def about(cls):
        return '''
        Postgres Reporter - (postgres)

        The Postgres reporter allows you to store events/metrics in Postgres tables. Note that (much like
        the Cassandra reporter) event tags and metric tags will be stored in separate tables. Tags can
        be queried for by session id *or* matched to their respective events/metrics by filtering on
        the associated event/metric uuid (generated automatically when an event or metric is submitted
        via update or submit reporter).

        '''

    async def init(self) -> PostgresAsyncConnector:

        metrics_tables = await self.metrics_table.create_table_configs()
        await self.connector.connect()
         
        await self.connector.setup([
            metrics_tables.get('model_table'),
            metrics_tables.get('tags_table')
        ])

        return self

    async def submit(self, metric) -> PostgresAsyncConnector:

        metric_insert = await self.metrics_table.create_insert_statements(
            metric,
            optional_field_values={
                'format': self.format,
                'session_id': self.session_id
            }
        )

        await self.connector.execute(metric_insert.get('model'))

        for tag_insert in metric_insert.get('tags'):
            await self.connector.execute(tag_insert)

        return await self.connector.commit() 

    async def close(self) -> PostgresAsyncConnector:
        await self.connector.close()
        return self



