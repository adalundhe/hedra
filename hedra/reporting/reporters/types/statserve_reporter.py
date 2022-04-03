
from __future__ import annotations
import uuid
from hedra.reporting.metrics import Metric
from hedra.reporting.connectors.types.statserve_connector import StatServeConnector as StatServe
from easy_logger import Logger

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

    async def update(self, event) -> list:
        await self.connector.execute({
            'type': 'update',
            **event.to_dict()
        })

        return await self.connector.commit()

    async def stream_updates(self, events) -> list:
        results = []
        async for result in self.connector.execute_stream(events):
            results.append(result)

        return results

    async def merge(self, connector) -> StatServeReporter:
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        if partial:
            query_type = 'stat_query'       
        else:
            query_type = 'field_query'

        await self.connector.execute({
            'key': key,
            'stat_type': stat_type,
            'stat_field': stat_field,
            'type': query_type
        })
        
        metrics = await self.connector.commit()

        return [
            Metric(
                reporter_type=self.format,
                stat=metric.get('metric_stat'),
                value=metric.get('metric_value'),
                metadata={            
                    'metric_name': metric.get('metric_name'),
                    'metric_host': metric.get('metric_host'),
                    'metric_url': metric.get('metric_url'),
                    'metric_type': metric.get('metric_type'),
                    'metric_tags':  metric.get('metric_tags')
                }
            ) for metric in metrics
        ]

    async def submit(self, metric) -> StatServeReporter:
        self.connector.connection.results += [metric.to_dict()]
        return self

    async def close(self) -> StatServeReporter:
        await self.connector.close()
        return self
