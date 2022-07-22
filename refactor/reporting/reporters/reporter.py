from .types import (
    DatadogReporter,
    JSONReporter,
    CassandraReporter,
    StatServeReporter,
    StatStreamReporter,
    MongoDBReporter,
    PostgresReporter,
    PrometheusReporter,
    KafkaReporter,
    RedisReporter,
    S3Reporter,
    GoogleCloudStorageReporter,
    SnowflakeReporter
)



class Reporter:

    reporter_types = {
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

    def __init__(self, reporter_config) -> None:
        self.type = reporter_config.get('reporter_type', 'statstream')
        self.selected = self.reporter_types.get(self.type)(
            reporter_config
        )

    @classmethod
    def about(cls):

        reporter_types = '\n\t'.join([f'- {reporter}' for reporter in cls.reporter_types.keys()])

        return f'''
        Reporters


        key-arguments:

        --reporter <reporter_type_to_use> (i.e. datadog, postgres, etc.)


        Hedra offers a comprehensive suite of integrations to export and store test results, including:

        {reporter_types}

        These integrations take the form of a consistent interface via Reporters.

        For more information on specific Reporters, run the command:

            hedra -about results:reporting:<reporter_type>

        
        Related topics:

        - runners
        - engines 

        '''
