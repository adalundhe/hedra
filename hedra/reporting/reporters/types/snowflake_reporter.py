from __future__ import annotations
from hedra.reporting.connectors.types.snowflake_connector import SnowflakeConnector
from .postgresql_reporter import PostgresReporter


class SnowflakeReporter(PostgresReporter):

    def __init__(self, config):
        super(SnowflakeReporter, self).__init__(config)
        self.format = 'snowflake'
        self.reporter_config = config
        self.connector = SnowflakeConnector(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        Snowflake Reporter - (snowflake)

        The Snowflake reporter allows you to store events and metrics via SnowflakeDB. As SnowflakeDB 
        draws heavily from Postgres-style SQL, the Snowflake Reporter utilizes much of the same functionality 
        as the Postgres reporter. As such, like the Postgres reporter event tags and metric tags will be stored 
        in separate tables. Tags can be queried for by session id *or* matched to their respective events/metrics 
        by filtering on the associated event/metric uuid (generated automatically when an event or metric is 
        submitted via update or submit reporter).

        '''

    async def init(self) -> SnowflakeReporter:
        await super().init()
        return self

    async def submit(self, metric) -> SnowflakeReporter:
        return await super().submit(metric) 

    async def close(self) -> SnowflakeReporter:
        await super().close()
        return self

