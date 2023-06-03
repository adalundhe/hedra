from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from typing import (
    Dict, 
    Union, 
    Any, 
    List,
    Callable,
    Coroutine
)

from .aws_lambda.aws_lambda_connector import AWSLambdaConnector
from .aws_lambda.aws_lambda_connector_config import AWSLambdaConnectorConfig
from .bigtable.bigtable_connector import BigTableConnector
from .bigtable.bigtable_connector_config import BigTableConnectorConfig
from .cassandra.cassandra_connector import CassandraConnector
from .cassandra.cassandra_connector_config import CassandraConnectorConfig
from .common.connector_type import ConnectorType
from .cosmosdb.cosmos_connector import CosmosDBConnector
from .cosmosdb.cosmos_connector_config import CosmosDBConnectorConfig
from .csv.csv_connector import CSVConnector
from .csv.csv_connector_config import CSVConnectorConfig
from .google_cloud_storage.google_cloud_storage_connector import GoogleCloudStorageConnector
from .google_cloud_storage.google_cloud_storage_connector_config import GoogleCloudStorageConnectorConfig
from .har.har_connector import HARConnector
from .har.har_connector_config import HARConnectorConfig
from .json.json_connector import JSONConnector
from .json.json_connector_config import JSONConnectorConfig
from .kafka.kafka_connector import KafkaConnector
from .kafka.kafka_connector_config import KafkaConnectorConfig
from .mongodb.mongodb_connector import MongoDBConnector
from .mongodb.mongodb_connector_config import MongoDBConnectorConfig
from .mysql.mysql_connector import MySQLConnector
from .mysql.mysql_connector_config import MySQLConnectorConfig
from .postgres.postgres_connector import PostgresConnection
from .postgres.postgres_connector_config import PostgresConnectorConfig
from .redis.redis_connector import RedisConnector
from .redis.redis_connector_config import RedisConnectorConfig
from .s3.s3_connector import S3Connector
from .s3.s3_connector_config import S3ConnectorConfig
from .snowflake.snowflake_connector import SnowflakeConnector
from .snowflake.snowflake_connector_config import SnowflakeConnectorConfig
from .sqlite.sqlite_connector import SQLiteConnector
from .sqlite.sqlite_connector_config import SQLiteConnectorConfig
from .xml.xml_connector import XMLConnector
from .xml.xml_connector_config import XMLConnectorConfig


ConnectorConfig = Union[
    AWSLambdaConnectorConfig,
    BigTableConnectorConfig,
    CassandraConnectorConfig,
    CosmosDBConnectorConfig,
    CSVConnectorConfig,
    GoogleCloudStorageConnectorConfig,
    HARConnectorConfig,
    JSONConnectorConfig,
    KafkaConnectorConfig,
    MongoDBConnectorConfig,
    MySQLConnectorConfig,
    PostgresConnectorConfig,
    RedisConnectorConfig,
    S3ConnectorConfig,
    SnowflakeConnectorConfig,
    SQLiteConnectorConfig,
    XMLConnectorConfig
]


class Connector:

    def __init__(
        self, 
        stage: str,
        connector_config: ConnectorConfig,
        parser_config: Config
    ) -> None:
        self._connectors: Dict[
            ConnectorType,
            Callable[
                [
                    Union[
                        AWSLambdaConnectorConfig,
                        BigTableConnectorConfig,
                        CassandraConnectorConfig,
                        CosmosDBConnectorConfig,
                        CSVConnectorConfig,
                        GoogleCloudStorageConnectorConfig,
                        HARConnectorConfig,
                        JSONConnectorConfig,
                        KafkaConnectorConfig,
                        MongoDBConnectorConfig,
                        MySQLConnectorConfig,
                        PostgresConnectorConfig,
                        RedisConnectorConfig,
                        S3ConnectorConfig,
                        SnowflakeConnectorConfig,
                        SQLiteConnectorConfig,
                        XMLConnectorConfig
                    ],
                    str,
                    Config
                ],
                Union[
                    AWSLambdaConnector,
                    BigTableConnector
                ]
            ]
        ] = {
            ConnectorType.AWSLambda: lambda config, stage, parser_config: AWSLambdaConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.BigTable: lambda config, stage, parser_config: BigTableConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.Cassandra: lambda config, stage, parser_config: CassandraConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.CosmosDB: lambda config, stage, parser_config: CosmosDBConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.CSV: lambda config, stage, parser_config: CSVConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.GCS: lambda config, stage, parser_config: GoogleCloudStorageConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.HAR: lambda config, stage, parser_config: HARConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.JSON: lambda config, stage, parser_config: JSONConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.Kafka: lambda config, stage, parser_config: KafkaConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.MongoDB: lambda config, stage, parser_config: MongoDBConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.MySQL: lambda config, stage, parser_config: MySQLConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.Postgres: lambda config, stage, parser_config: PostgresConnection(
                config,
                stage,
                parser_config
            ),
            ConnectorType.Redis: lambda config, stage, parser_config: RedisConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.S3: lambda config, stage, parser_config: S3Connector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.Snowflake: lambda config, stage, parser_config: SnowflakeConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.SQLite: lambda config, stage, parser_config: SQLiteConnector(
                config,
                stage,
                parser_config
            ),
            ConnectorType.XML: lambda config, stage, parser_config: XMLConnector(
                config,
                stage,
                parser_config
            )
        }   

        self.selected = self._connectors.get(
            connector_config.connector_type
        )(
            connector_config,
            stage,
            parser_config
        )

        self.stage = stage
        self.parser_config = parser_config
        self.connected = False

    async def connect(self):
        await self.selected.connect()
        self.connected = True

    async def load_execute_stage_summary(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, ExecuteStageSummaryValidator]:
        return await self.selected.load_execute_stage_summary(
            options=options
        )

    async def load_actions(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, List[ActionHook]]:
        return await self.selected.load_actions(
            options=options
        )
    
    async def load_results(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, ResultsSet]:
        return await self.selected.load_results(
            options=options
        )
    
    async def load_data(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, Any]:
        return await self.load_data(
            options=options
        )
    
    async def close(self):
        self.connected = False
        return await self.selected.close()