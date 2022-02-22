
from __future__ import annotations
import uuid
from zebra_automate_connect.types.statstream_connector import StatStreamConnector as StatsStream

class StatStreamReporter:

    def __init__(self, config):
        self.format = 'statstream'
        self.reporter_config = config

        self.session_id = uuid.uuid4()
        
        stream_config = self.reporter_config.get('stream_config', {})
        if stream_config.get('stream_name') is None:
            self.stream_name = str(self.session_id)
            stream_config['stream_name'] = self.stream_name
        
        else:
            self.stream_name = stream_config.get('stream_name')


        self.reporter_config['stream_config'] = stream_config

        self.connector = StatsStream(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        Statstream Reporter - (statstream)

        The Statstream reporter allows you to submit events to the Statstream library for aggregation and
        to retrieve these aggregate events and convert them to Metrics, which are output to JSON file.
        The Statstream reporter is ideal for local use - as it does not involve running an additional
        server, it will both free up additional computational resources for Hedra to use while testing
        and may compute aggregate results more quickly (no overhead of requests).

        The Statstream reporter *should not* be used for distriuted execution, as the reporter has no
        way to aggregate or retrieve results across workers. Instead use the Statserve reporter.

        '''

    async def init(self) -> StatStreamReporter:
        await self.connector.connect()
        return self

    async def update(self, event) -> list:
        await self.connector.execute({
            'type': 'update',
            'stream_name': self.stream_name,
            **event.to_dict()
        })

        return await self.connector.commit()

    async def merge(self, connector) -> StatStreamReporter:
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        if partial:
            query_type = 'stat_query'       
        else:
            query_type = 'field_query'

        await self.connector.execute({
            'stream_name': self.stream_name,
            'key': key,
            'stat_type': stat_type,
            'stat_field': stat_field,
            'type': query_type
        })

        return await self.connector.commit()

    async def submit(self, metric) -> StatStreamReporter:
        self.connector.connection.results += [metric.to_dict()]
        return self

    async def close(self) -> StatStreamReporter:
        await self.connector.close()
        return self


