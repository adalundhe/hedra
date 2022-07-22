import uuid
from easy_logger import Logger
from hedra.reporting.events import Event
from hedra.reporting.metrics import Metric
from hedra.reporting.connectors.types.statstream_connector import StatStreamConnector
from statstream.streaming import StatStream
from async_tools.datatypes import AsyncList
from hedra.command_line import CommandLine
from .reporters import Reporter


class Handler:


    def __init__(self, config: CommandLine):
        self.config = config
        self.runner_mode = self.config.runner_mode
        self.batches = 0

        self.session_id = str(uuid.uuid4())

        self.connector = StatStreamConnector({
            'stream_config': {
                'stream_name': self.session_id
            }
        })

        self.stream = StatStream()

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.reporter = Reporter(config.reporter_config)

    async def initialize_reporter(self):
        await self.reporter.selected.init()

    async def merge(self, aggregate_events):

        for aggregate_event in aggregate_events:
            field_name = aggregate_event.get('metadata').get('event_name')

            await self.stream.merge_stream(
                aggregate_event, 
                field_name=field_name
            )

    async def get_stats(self):
        return await self.stream.get_stream_stats()
            
    async def aggregate(self, actions, bar=None):

        event_names = set()
        await self.connector.connect()

        async for action in AsyncList(actions):
            event = Event(action)
            await event.assert_response()
    
            event_names.add(event.data.name)

            await self.connector.execute({
                'type': 'update',
                'stream_name': self.session_id,
                **event.to_dict()
            })

            if bar:
                bar()

        await self.connector.commit()

        for event_name in event_names:
            await self.connector.execute({
                'stream_name': self.session_id,
                'key': event_name,
                'type': 'field_query'
            })
                
        return await self.connector.commit()

    async def submit(self, aggregate_events):
        if self.runner_mode == 'worker':
            self.session_logger.warning('Warning: Results cannot be generated for distributed workers.')
        else:
            for aggregate_event in aggregate_events.values():
                metadata = aggregate_event.get('metadata')
                stats = aggregate_event.get('stats')
                quantiles = aggregate_event.get('quantiles')
                counts = aggregate_event.get('counts')

                metrics = []
                
                for stat_name, stat in stats.items():

                    metric = Metric(
                        reporter_type=self.reporter.type,
                        stat=stat_name,
                        value=stat,
                        metadata=metadata
                    )

                    metrics.append(metric)

                for quantile_name, quantile in quantiles.items():
                    metric = Metric(
                        reporter_type=self.reporter.type,
                        stat=quantile_name,
                        value=quantile,
                        metadata=metadata
                    )

                    metrics.append(metric)

                for count_name, count in counts.items():
                    metric = Metric(
                        reporter_type=self.reporter.type,
                        stat=count_name,
                        value=count,
                        metadata=metadata
                    )

                    metrics.append(metric)
                    

                for metric in metrics:
                    await self.reporter.selected.submit(metric)

        await self.reporter.selected.close()

        return True
