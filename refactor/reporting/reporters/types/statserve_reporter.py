
from __future__ import annotations
import uuid
from hedra.reporting.connectors.types.statserve_connector import StatServeConnector as StatServe

class StatServeReporter:

    def __init__(self, config):
        self.format = 'statserve'
        self.reporter_config = config
        self.session_id = uuid.uuid4()

        if self.reporter_config.get('save_to_file') is None:
            self.reporter_config['save_to_file'] = True
        
        stream_config = self.reporter_config.get('stream_config', {})

        if stream_config.get('stream_name') is None:
            stream_config['stream_name'] = "hedra"

        self.reporter_config['stream_config'] = stream_config

        self.connector = StatServe(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        Stataserve Reporter - (statserve)

        The Statserve reporter allows you to submit events and metrics to Statserve. Unlike most
        other reporters, the Statserve reporter requires no additional effort to retrieve aggregated
        event results, as Statserve will compute these aggregate results as each event is submitted.
        However, Statserve offers only in-memory storage, meaning that once aggregation is complete
        and the aggregate metrics are retrieved, they should be stored elsewhere as soon as possible.
        
        If Statserve is specified as the submit connector, Statserve will fetch aggregate events,
        parse and convert them into Metrics, and store these metrics as a JSON file.
        
        '''

    async def init(self) -> StatServeReporter:
        await self.connector.connect()

    async def submit(self, metric) -> StatServeReporter:
        self.connector.connection.results += [metric.to_dict()]
        return self

    async def close(self) -> StatServeReporter:
        await self.connector.close()
        return self
