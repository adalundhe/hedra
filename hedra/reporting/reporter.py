import uuid
import threading
import os
from typing import Any, List, TypeVar, Union
from hedra.logging import HedraLogger
from hedra.plugins.types.reporter.reporter_config import ReporterConfig
from .types import ReporterTypes
from .types import (
    AWSLambda,
    AWSLambdaConfig,
    AWSTimestream,
    AWSTimestreamConfig,
    BigQuery,
    BigQueryConfig,
    BigTable,
    BigTableConfig,
    Cassandra,
    CassandraConfig,
    Cloudwatch,
    CloudwatchConfig,
    CosmosDB,
    CosmosDBConfig,
    CSV,
    CSVConfig,
    Datadog,
    DatadogConfig,
    DogStatsD,
    DogStatsDConfig,
    GoogleCloudStorage,
    GoogleCloudStorageConfig,
    Graphite,
    GraphiteConfig,
    Honeycomb,
    HoneycombConfig,
    InfluxDB,
    InfluxDBConfig,
    JSON,
    JSONConfig,
    Kafka,
    KafkaConfig,
    MongoDB,
    MongoDBConfig,
    MySQL,
    MySQLConfig,
    Netdata,
    NetdataConfig,
    NewRelic,
    NewRelicConfig,
    Postgres,
    PostgresConfig,
    Prometheus,
    PrometheusConfig,
    Redis,
    RedisConfig,
    S3,
    S3Config,
    Snowflake,
    SnowflakeConfig,
    SQLite,
    SQLiteConfig,
    StatsD,
    StatsDConfig,
    Telegraf,
    TelegrafConfig,
    TelegrafStatsD,
    TelegrafStatsDConfig,
    TimescaleDB,
    TimescaleDBConfig
)

ReporterType = TypeVar(
    'ReporterType',
    AWSLambdaConfig,
    AWSTimestreamConfig,
    BigQueryConfig, 
    BigTableConfig, 
    CassandraConfig,
    CloudwatchConfig,
    CosmosDBConfig,
    CSVConfig,
    DatadogConfig,
    DogStatsDConfig,
    GoogleCloudStorageConfig,
    GraphiteConfig,
    HoneycombConfig,
    InfluxDBConfig,
    JSONConfig,
    KafkaConfig,
    MongoDBConfig,
    MySQLConfig,
    NetdataConfig,
    NewRelicConfig,
    PostgresConfig,
    PrometheusConfig,
    RedisConfig,
    S3Config,
    SnowflakeConfig,
    SQLiteConfig,
    StatsDConfig,
    TelegrafConfig,
    TelegrafStatsDConfig,
    TimescaleDBConfig
)

class Reporter:
    reporters = {
            ReporterTypes.AWSLambda: lambda config: AWSLambda(config),
            ReporterTypes.AWSTimestream: lambda config: AWSTimestream(config),
            ReporterTypes.BigQuery: lambda config: BigQuery(config),
            ReporterTypes.BigTable: lambda config: BigTable(config),
            ReporterTypes.Cassandra: lambda config: Cassandra(config),
            ReporterTypes.Cloudwatch: lambda config: Cloudwatch(config),
            ReporterTypes.CosmosDB: lambda config: CosmosDB(config),
            ReporterTypes.CSV: lambda config: CSV(config),
            ReporterTypes.Datadog: lambda config: Datadog(config),
            ReporterTypes.DogStatsD: lambda config: DogStatsD(config),
            ReporterTypes.GCS: lambda config: GoogleCloudStorage(config),
            ReporterTypes.Graphite: lambda config: Graphite(config),
            ReporterTypes.Honeycomb: lambda config: Honeycomb(config),
            ReporterTypes.InfluxDB: lambda config: InfluxDB(config),
            ReporterTypes.JSON: lambda config: JSON(config),
            ReporterTypes.Kafka: lambda config: Kafka(config),
            ReporterTypes.MongoDB: lambda config: MongoDB(config),
            ReporterTypes.MySQL: lambda config: MySQL(config),
            ReporterTypes.Netdata: lambda config: Netdata(config),
            ReporterTypes.NewRelic: lambda config: NewRelic(config),
            ReporterTypes.Postgres: lambda config: Postgres(config),
            ReporterTypes.Prometheus: lambda config: Prometheus(config),
            ReporterTypes.Redis: lambda config: Redis(config),
            ReporterTypes.S3: lambda config: S3(config),
            ReporterTypes.Snowflake: lambda config: Snowflake(config),
            ReporterTypes.SQLite: lambda config: SQLite(config),
            ReporterTypes.StatsD: lambda config: StatsD(config),
            ReporterTypes.Telegraf: lambda config: Telegraf(config),
            ReporterTypes.TelegrafStatsD: lambda config: TelegrafStatsD(config),
            ReporterTypes.TimescaleDB: lambda config: TimescaleDB(config)
        }

    def __init__(self, reporter_config: Union[ReporterConfig, ReporterType]) -> None:
        self.reporter_id = str(uuid.uuid4())
        self.logger = HedraLogger()
        self.logger.initialize()

        self.graph_name: str=None
        self.graph_id: str=None
        self.stage_name: str=None
        self.stage_id: str=None
        self.metadata_string: str=None
        self.thread_id = threading.current_thread().ident
        self.process_id = os.getpid()

        if reporter_config is None:
            reporter_config = JSONConfig()

        self.reporter_type = reporter_config.reporter_type
        self.reporter_type_name = self.reporter_type.name.capitalize()
        self.reporter_config = reporter_config
        
        selected_reporter = self.reporters.get(self.reporter_type)
        if selected_reporter is None:
            self.selected_reporter = JSON(reporter_config)

        else:
            self.selected_reporter = selected_reporter(reporter_config)

    async def connect(self):
        self.metadata_string = f'Graph - {self.graph_name}:{self.graph_id} - thread:{self.thread_id} - process:{self.process_id} - Stage: {self.stage_name}:{self.stage_id} - Reporter: {self.reporter_type_name}:{self.reporter_id} - '
        self.selected_reporter.metadata_string = self.metadata_string

        await self.logger.filesystem.aio.create_logfile('hedra.reporting.log')
        self.logger.filesystem.create_filelogger('hedra.reporting.log')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting')
        await self.selected_reporter.connect()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected')

    async def submit_common(self, metrics: List[Any]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting {len(metrics)} shared metrics')
        await self.selected_reporter.submit_common(metrics)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted {len(metrics)} shared metrics')

    async def submit_events(self, events: List[Any]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting {len(events)} events')
        await self.selected_reporter.submit_events(events)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted {len(events)} events')

    async def submit_metrics(self, metrics: List[Any]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting {len(metrics)} metrics')
        await self.selected_reporter.submit_metrics(metrics)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted {len(metrics)} metrics')

    async def submit_custom(self, metrics: List[Any]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting {len(metrics)} custom metrics')
        await self.selected_reporter.submit_custom(metrics)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted {len(metrics)} custom metrics')

    async def submit_errors(self, metrics: List[Any]):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting {len(metrics)} errors')
        await self.selected_reporter.submit_errors(metrics)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted {len(metrics)} errors')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing')
        await self.selected_reporter.close()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed')