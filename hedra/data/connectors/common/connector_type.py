from enum import Enum


class ConnectorType(Enum):
    AWSLambda='aws_lambda'
    BigTable='bigtable'
    Cassandra='cassandra'
    CosmosDB='cosmosdb'
    CSV='csv'
    GCS='gcs'
    HAR='har'
    JSON='json'
    Kafka='kafka'
    MongoDB='mongodb'
    MySQL='mysql'
    Postgres='postgres'
    Redis='redis'
    S3='s3'
    Snowflake='snowflake'
    SQLite='sqlite'
    TimescaleDB='timescaledb'
    XML='xml'