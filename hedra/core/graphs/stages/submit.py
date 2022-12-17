from typing import Generic, TypeVar
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.reporter.reporter_config import ReporterConfig
from hedra.reporting import (
    Reporter,
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
from .stage import Stage

T = TypeVar('T')

class Submit(Stage, Generic[T]):
    stage_type=StageTypes.SUBMIT
    config: (
        BigQueryConfig | BigTableConfig | CassandraConfig | CloudwatchConfig | CosmosDBConfig |
        CSVConfig | DatadogConfig | DogStatsDConfig | GoogleCloudStorageConfig | GraphiteConfig | HoneycombConfig |
        InfluxDBConfig | JSONConfig | KafkaConfig | MongoDBConfig | MySQLConfig | NetdataConfig |
        NewRelicConfig | PostgresConfig | PrometheusConfig | RedisConfig | S3Config | SnowflakeConfig |
        SQLiteConfig | StatsDConfig | TelegrafConfig | TelegrafStatsDConfig | TimescaleDBConfig
    ) = None
    submit_events: bool = False
    
    def __init__(self) -> None:
        super().__init__()
        self.summaries = {}
        self.events = []
        self.reporter: Reporter = None

    @Internal()
    async def run(self):

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Initializing results submission')

        reporter_plugins = self.plugins_by_type.get(PluginType.REPORTER)

        for plugin_name, plugin in reporter_plugins.items():
            Reporter.reporters[plugin_name] = plugin

            if isinstance(self.config, plugin.config):
                self.config.reporter_type = plugin_name
            
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Reporter plugin - {plugin_name}')
        
        self.reporter = Reporter(self.config)
        self.reporter.graph_name = self.graph_name
        self.reporter.graph_id = self.graph_id
        self.reporter.stage_name = self.name
        self.reporter.stage_id = self.stage_id
        
        reporter_name = self.reporter.reporter_type.name.capitalize()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Submitting results via - {reporter_name}:{self.reporter.reporter_id} - reporter')
        await self.logger.spinner.append_message(f'Submitting results via - {reporter_name} - reporter')

        await self.reporter.connect()

        session_total = self.summaries.get('session_total', 0)
        if self.submit_events:

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{self.reporter.reporter_id} - Submitting - {session_total} - Events')
            await self.reporter.submit_events(self.events)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{self.reporter.reporter_id} - Submitted - {session_total} - Events')

        metrics = []

        stage_summaries = self.summaries.get('stages', {})
        for stage in stage_summaries.values():
            metrics.extend(list(
                stage.get('actions', {}).values()
            ))

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{self.reporter.reporter_id} - Submitting Metrics')

        await self.reporter.submit_common(metrics)
        await self.reporter.submit_metrics(metrics)
        await self.reporter.submit_errors(metrics)
        await self.reporter.submit_custom(metrics)
        await self.reporter.close()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{self.reporter.reporter_id} - Completed Metrics submission')
        await self.logger.spinner.set_default_message(f'Successfully submitted the results for {session_total} actions via {reporter_name} reporter')
        

