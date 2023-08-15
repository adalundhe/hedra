from hedra.core.engines.types.common.types import RequestTypes
from typing import (
    Dict, 
    Callable, 
    Union,
    Iterable
)
try:
    from cassandra.cqlengine.models import Model
    has_connector = True

except Exception:
    Model = None
    has_connector = False

from .action_schemas import (
    CassandraGraphQLActionSchema,
    CassandraGraphQLHTTP2ActionSchema,
    CassandraGRPCActionSchema,
    CassandraHTTPActionSchema,
    CassandraHTTP2ActionSchema,
    CassandraHTTP3ActionSchema,
    CassandraPlaywrightActionSchema,
    CassandraTaskSchema,
    CassandraUDPActionSchema,
    CassandraWebsocketActionSchema
)

from .result_schemas import (
    CassandraGraphQLResultSchema,
    CassandraGraphQLHTTP2ResultSchema,
    CassandraGRPCResultSchema,
    CassandraHTTPResultSchema,
    CassandraHTTP2ResultSchema,
    CassandraHTTP3ResultSchema,
    CassandraPlaywrightResultSchema,
    CassandraTaskResultSchema,
    CassandraUDPResultSchema,
    CassandraWebsocketResultSchema
)


class CassandraSchemaSet:

    def __init__(self) -> None:
        self._action_schemas: Dict[
            RequestTypes,
            Callable[
                [str],
                Union[
                    CassandraGraphQLActionSchema,
                    CassandraGraphQLHTTP2ActionSchema,
                    CassandraGRPCActionSchema,
                    CassandraHTTPActionSchema,
                    CassandraHTTP2ActionSchema,
                    CassandraHTTP3ActionSchema,
                    CassandraPlaywrightActionSchema,
                    CassandraTaskSchema,
                    CassandraUDPActionSchema,
                    CassandraWebsocketActionSchema
                ]
            ]
        ] = {
            RequestTypes.GRAPHQL: lambda table_name: CassandraGraphQLActionSchema(table_name),
            RequestTypes.GRAPHQL_HTTP2: lambda table_name: CassandraGraphQLHTTP2ActionSchema(table_name),
            RequestTypes.GRPC: lambda table_name: CassandraGRPCActionSchema(table_name),
            RequestTypes.HTTP: lambda table_name: CassandraHTTPActionSchema(table_name),
            RequestTypes.HTTP2: lambda table_name: CassandraHTTP2ActionSchema(table_name),
            RequestTypes.HTTP3: lambda table_name: CassandraHTTP3ActionSchema(table_name),
            RequestTypes.PLAYWRIGHT: lambda table_name: CassandraPlaywrightActionSchema(table_name),
            RequestTypes.TASK: lambda table_name: CassandraTaskSchema(table_name),
            RequestTypes.UDP: lambda table_name: CassandraUDPActionSchema(table_name),
            RequestTypes.WEBSOCKET: lambda table_name: CassandraWebsocketActionSchema(table_name)
        }

        self._results_schemas: Dict[
            RequestTypes,
            Callable[
                [str],
                Union[
                    CassandraGraphQLResultSchema,
                    CassandraGraphQLHTTP2ResultSchema,
                    CassandraGRPCResultSchema,
                    CassandraHTTPResultSchema,
                    CassandraHTTP2ResultSchema,
                    CassandraHTTP3ResultSchema,
                    CassandraPlaywrightResultSchema,
                    CassandraTaskResultSchema,
                    CassandraUDPResultSchema,
                    CassandraWebsocketResultSchema
                ]
            ]
        ] = {
            RequestTypes.GRAPHQL: lambda table_name: CassandraGraphQLResultSchema(table_name),
            RequestTypes.GRAPHQL_HTTP2: lambda table_name: CassandraGraphQLHTTP2ResultSchema(table_name),
            RequestTypes.GRPC: lambda table_name: CassandraGRPCResultSchema(table_name),
            RequestTypes.HTTP: lambda table_name: CassandraHTTPResultSchema(table_name),
            RequestTypes.HTTP2: lambda table_name: CassandraHTTP2ResultSchema(table_name),
            RequestTypes.HTTP3: lambda table_name: CassandraHTTP3ResultSchema(table_name),
            RequestTypes.PLAYWRIGHT: lambda table_name: CassandraPlaywrightResultSchema(table_name),
            RequestTypes.TASK: lambda table_name: CassandraTaskResultSchema(table_name),
            RequestTypes.UDP: lambda table_name: CassandraUDPResultSchema(table_name),
            RequestTypes.WEBSOCKET: lambda table_name: CassandraWebsocketResultSchema(table_name)
        }

    def get_action_schema(
        self, 
        request_type: RequestTypes,
        table_name: str
    ):
        return self._action_schemas.get(
            request_type,
            CassandraHTTPActionSchema
        )(table_name)
    
    def get_result_schema(
        self, 
        request_type: RequestTypes,
        table_name: str
    ):
        return self._results_schemas.get(
            request_type,
            CassandraHTTPResultSchema
        )(table_name)

    def action_schemas(
        self, 
        table_name: str
    ):
        for schema in self._action_schemas.values():
            cassandra_schema = schema(table_name)

            request_type = cassandra_schema.type.lower()
            assembled_table_name = f'{table_name}_{request_type}'

            yield assembled_table_name, cassandra_schema.actions_table
    
    def results_schemas(
        self, 
        table_name: str
    ):
        for schema in self._results_schemas.values():
            cassandra_schema = schema(table_name)

            request_type = cassandra_schema.type.lower()
            assembled_table_name = f'{table_name}_{request_type}'

            yield assembled_table_name, cassandra_schema.results_table
