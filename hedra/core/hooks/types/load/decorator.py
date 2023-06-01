import functools
import os
from typing import Tuple, Union
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from hedra.data.connectors.connector import (
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
)
from .validator import LoadHookValidator


@registrar(HookType.LOAD)
def load(
    *names: Tuple[str, ...], 
    loader: Union[
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
    ]=None, 
    order: int=1,
    skip: bool=False
):
    
    LoadHookValidator(
        names=names,
        loader=loader,
        order=order,
        skip=skip
    )
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper