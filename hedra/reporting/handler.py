import uuid
from easy_logger import Logger
from hedra.reporting.events import Event
from hedra.reporting.metrics import Metric
from hedra.reporting.connectors.types.statstream_connector import StatStreamConnector
from statstream.streaming import StatStream
from async_tools.datatypes import AsyncList
from hedra.command_line import CommandLine
from .reporters.types import (
    DatadogReporter,
    JSONReporter,
    CassandraReporter,
    StatStreamReporter,
    StatServeReporter,
    MongoDBReporter,
    PostgresReporter,
    PrometheusReporter,
    KafkaReporter,
    S3Reporter,
    GoogleCloudStorageReporter,
    SnowflakeReporter,
    RedisReporter
)



class Handler:

    reporters = {
        'datadog': DatadogReporter,
        'json': JSONReporter,
        'cassandra': CassandraReporter,
        'statserve': StatServeReporter,
        'statstream': StatStreamReporter,
        'mongodb': MongoDBReporter,
        'postgres': PostgresReporter,
        'prometheus': PrometheusReporter,
        'kafka': KafkaReporter,
        's3': S3Reporter,
        'gcs': GoogleCloudStorageReporter,
        'snowflake': SnowflakeReporter,
        'redis': RedisReporter
    }

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
        self.reporter_config = config.reporter_config

        self.reporter_type = self.reporter_config.get('reporter_type', 'statserve')
        self.reporter = self.reporters.get(self.reporter_type)(
            self.reporter_config
        )

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


    async def connect(self):
        await self.reporter.init()

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
                
                for stat_name, stat in stats.items():

                    metric = Metric(
                        reporter_type=self.reporter_type,
                        stat=stat_name,
                        value=stat,
                        metadata=metadata
                    )

                    await self.reporter.submit(metric)

        await self.reporter.close()

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