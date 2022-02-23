from easy_logger import Logger
from .types import (
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

class ReporterStore:

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

    def __init__(self, config=None):
        if config is None:
            raise Exception('Error: Config object required.')
        
        logger = Logger()
        self.session_logger = logger.generate_logger()

        self._update_reporter_type = config.get('update_reporter_type', 'statserve')
        self.session_logger.debug('Selected - {reporter_type} - as update reporter'.format(
            reporter_type=self._update_reporter_type
        ))

        self._fetch_reporter_type = config.get('fetch_reporter_type', 'statserve')
        self.session_logger.debug('Selected - {reporter_type} - as fetch reporter'.format(
            reporter_type=self._fetch_reporter_type
        ))

        self._submit_reporter_type = config.get('submit_reporter_type', 'statserve')
        self.session_logger.debug('Selected - {reporter_type} - as submit reporter'.format(
            reporter_type=self._submit_reporter_type
        ))

        self.update_reporter = self.reporters.get(self._update_reporter_type)(
            config.get('update_config', {})
        )
        self.fetch_reporter = self.reporters.get(self._fetch_reporter_type)(
            config.get('fetch_config', {})
        )
        self.submit_reporter = self.reporters.get(self._submit_reporter_type)(
            config.get('submit_config', {})
        )

        if self._fetch_reporter_type == 'statstream':
            self.fetch_reporter = self.update_reporter

        if self._submit_reporter_type == 'statstream':
            self.submit_reporter = self.update_reporter

    async def init(self):
        await self.update_reporter.init()
        await self.fetch_reporter.init()
        await self.submit_reporter.init()

    async def update_stream(self, events):
        parsed_events = []
        for event in events:
            if event.format != self.update_reporter.format:
                self.session_logger.debug('Converting event of type - {from_type} - to type - {to_type}'.format(
                    from_type=event.format,
                    to_format=self.update_reporter.format
                ))
                event = await event.convert(self.update_reporter.format)

            parsed_events += [event]

        return await self.update_reporter.stream_updates(parsed_events)

    async def update(self, event):
        if event.format != self.update_reporter.format:
            self.session_logger.debug('Converting event of type - {from_type} - to type - {to_type}'.format(
                from_type=event.format,
                to_format=self.update_reporter.format
            ))
            event = await event.convert(self.update_reporter.format)

        return await self.update_reporter.update(event)

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False):
        self.session_logger.debug('Fetching summarized results for key - {key}'.format(
            key=key,
            stat_type=stat_type,
            stat_field=stat_field,
            partial=partial
        ))
        return await self.fetch_reporter.fetch(key=key)

    async def merge(self, reporter):
        self.session_logger.info('Executing merge of update reporters...')
        await self.update_reporter.merge(reporter.update_reporter)
        return self

    async def submit(self, metric):
        if metric.format != self.submit_reporter.format:
            self.session_logger.debug('Converting metric of type - {from_type} - to type - {to_type}'.format(
                from_type=metric.format,
                to_type=self.submit_reporter.format
            ))
            metric = await metric.convert(self.submit_reporter.format)

        return await self.submit_reporter.submit(metric)

    async def close(self):
        self.session_logger.debug('Closing StatsReport session. Goodbye!')
        await self.update_reporter.close()
        await self.fetch_reporter.close()
        await self.submit_reporter.close()
        return self
