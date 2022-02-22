from __future__ import annotations
import uuid
from zebra_automate_connect.types.cassandra_connector import CassandraConnector as Cassandra
from hedra.reporting.events.types import CassandraEvent
from hedra.reporting.metrics.types import CassandraMetric
from .utils.helpers import CassandraHelper

class CassandraReporter:

    def __init__(self, config):
        self.format = 'cassandra'

        self.reporter_config = config
        self.cassandra_helper = CassandraHelper(
            keyspace_config=self.reporter_config.get('keyspace_config', 'hedra')
        )
        self.connector = Cassandra(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        Cassandra Reporter - (cassandra)

        The Cassandra reporter allows you to submit events and metrics to a running Cassandra instance. By default,
        events and metrics will be stored under the "hedra" keyspace, with events stored under the "events" table,
        event tags stored under the "event_tags" table, metrics stored under the "metrics" table, and metric tags
        stored under the "metric_tags" table.

        '''

    async def init(self) -> CassandraReporter:
        self.events_table = self.reporter_config.get('events', 'events')
        self.events_tags_table = self.reporter_config.get('event_tags', 'event_tags')
        self.metrics_table = self.reporter_config.get('metrics', 'metrics')
        self.metrics_tags_table = self.reporter_config.get('metrics_tags', 'metrics_tags')

        self.cassandra_helper.set_keyspace()

        await self.connector.connect()
        
        await self.connector.setup({
            "keyspace": self.cassandra_helper.keyspace_config,
            "tables": [
                self.cassandra_helper.as_table(
                    table_name=self.events_table,
                    data=CassandraEvent
                ),
                self.cassandra_helper.as_table(
                    table_name=self.metrics_table,
                    data=CassandraMetric
                ),
                self.cassandra_helper.tags_as_table(table_name=self.events_tags_table),
                self.cassandra_helper.tags_as_table(table_name=self.metrics_tags_table)
            ]
        })

        return self

    async def update(self, event) -> list:

        insert_query = await self.cassandra_helper.to_record(
            table=self.events_table,
            data=event,
            timestamp=event.get_utc_time()
        )

        await self.connector.execute(insert_query.get('table'))

        for insert_tag_query in insert_query.get('tags_table'):
            await self.connector.execute(insert_tag_query)

        return await self.connector.commit()

    async def merge(self, connector) -> CassandraReporter:
        #TODO: Implement merge.
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        query = await self.cassandra_helper.to_query(
            table=self.events_table,
            tags_table=self.events_tags_table
        )

        await self.connector.execute(
            query.get('table')
        )
        
        event = await self.connector.commit()

        await self.connector.execute(
            query.get('tags_table')
        )
        event_tags = await self.connector.commit()

        return [
            {
                **event,
                'tags': event_tags
            }
        ]

    async def submit(self, metric) -> CassandraReporter:

        insert_metric_query = await self.cassandra_helper.to_record(
            table=self.metrics_table,
            data=metric,
            timestamp=metric.get_utc_time()
        )

        await self.connector.execute(insert_metric_query.get('table'))
        for insert_tag_query in insert_metric_query.get('tags_table'):
            await self.connector.execute(insert_tag_query)

        return self 

    async def close(self) -> CassandraReporter:
        await self.connector.close()
        return self