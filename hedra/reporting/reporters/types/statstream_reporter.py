
from __future__ import annotations
import uuid
from hedra.reporting.connectors.types.statstream_connector import StatStreamConnector as StatsStream

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

    async def submit(self, metric) -> StatStreamReporter:
        self.connector.connection.results += [metric.to_dict()]
        return self

    async def close(self) -> StatStreamReporter:
        await self.connector.close()
        return self


