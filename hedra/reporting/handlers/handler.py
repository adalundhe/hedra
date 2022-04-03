import uuid
from easy_logger import Logger
from hedra.reporting import Reporter
from hedra.reporting.events import Event
from hedra.reporting.connectors.types.statstream_connector import StatStreamConnector
from statstream.streaming import StatStream
from async_tools.datatypes import AsyncList


class Handler:

    def __init__(self, config):
        self.config = config
        self.runner_mode = self.config.runner_mode
        self.reporter = Reporter()
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


    @classmethod
    def about(cls):
        return '''
        Results

        key-arguments:

        --reporter-config-filepath <path_to_reporter_config_JSON_file> (required but defaults to the directory in which hedra is running)

        Hedra will automatically process results at the end of execution based on the configuration stored in
        and provided by the config.json. For exmaple, a file title reporter-config.json containing:

        {
            "reporter_config": {
                "update_connector_type": "statserve",
                "fetch_connector_type": "statserve",
                "submit_connector_type": "statserve",
                "update_config": {
                    "stream_config": {
                        "stream_name": "hedra",
                        "fields": {}
                    }
                },
                "fetch_config": {
                    "stream_config": {
                        "stream_name": "hedra",
                        "fields": {}
                    }
                },
                "submit_config": {
                    "save_to_file": true,
                    "stream_config": {
                        "stream_name": "hedra",
                        "fields": {}
                    }
                }
            }
        }

        will tell Hedra to use Statserve for results aggregation/computation and then output a JSON file of aggregated 
        metrics and results at the end.

        Depdending on the executor seleted, Hedra will either attempt to connect to the specified reporter resources
        prior to execution or (specifically for the parallel executor) after execution has completed. For more information
        on how reporters work, run the command:

            hedra --about results:reporting

        Related Topics:

        - runners
        
        '''


    async def on_config(self, reporter_config):
        await self.reporter.initialize(reporter_config, log_level=self.config.log_level)

    async def merge(self, aggregate_events):

        for aggregate_event in aggregate_events:
            field_name = aggregate_event.get('metadata').get('event_name')

            await self.stream.merge_stream(
                aggregate_event, 
                field_name=field_name
            )

    async def get_stats(self):
        return await self.stream.get_stream_stats()
            
    async def aggregate(self, actions):

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

        await self.connector.commit()

        for event_name in event_names:
            await self.connector.execute({
                'stream_name': self.session_id,
                'key': event_name,
                'type': 'field_query'
            })
                
        return await self.connector.commit()

    async def on_exit(self, aggregate_events):
        if self.runner_mode == 'worker':
            self.session_logger.warning('Warning: Results cannot be generated for distributed workers.')
        else:
            await self.reporter.on_output(aggregate_events)

        return True

    async def serialize(self, actions):
        async for batch in AsyncList(actions):
            async for action in AsyncList(batch):
                event = Event(action)
                await event.assert_response()

                yield {
                    'type': 'update',
                    **event.to_dict()
                }