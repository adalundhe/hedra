import uvloop
uvloop.install()


from typing import TypeVar
import click
import asyncio
from hedra.core.engines.types.registry import engines_registry
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.grpc import (
    GRPCAction,
    MercuryGRPCClient
)
from hedra.core.engines.types.graphql import (
    GraphQLAction,
    MercuryGraphQLClient
)
from hedra.core.engines.types.graphql_http2 import (
    GraphQLHTTP2Action,
    MercuryGraphQLHTTP2Client
)
from hedra.core.engines.types.http import (
    HTTPAction,
    MercuryHTTPClient
)
from hedra.core.engines.types.http2 import (
    HTTP2Action,
    MercuryHTTP2Client
)
from hedra.core.engines.types.playwright import (
    MercuryPlaywrightClient
)
from hedra.core.engines.types.udp import (
    UDPAction,
    MercuryUDPClient
)
from hedra.core.engines.types.websocket import (
    WebsocketAction,
    MercuryWebsocketClient
)


T = TypeVar(
    'T', 
    MercuryGRPCClient,
    MercuryGraphQLHTTP2Client, 
    MercuryGraphQLClient, 
    MercuryHTTP2Client, 
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryUDPClient,
    MercuryWebsocketClient
)

@click.command("Ping the specified uri to ensure it can be reached.")
@click.argument('uri')
@click.option('--engine', default='http', type=str)
@click.option('--timeout', default=60, type=int)
def ping(uri: str, engine: str, timeout: int):

    engine_types_map = {
        'http': RequestTypes.HTTP,
        'http2': RequestTypes.HTTP2,
        'grpc': RequestTypes.GRPC,
        'graphql': RequestTypes.GRAPHQL,
        'graphql-http2': RequestTypes.GRAPHQL_HTTP2,
        'playwright': RequestTypes.PLAYWRIGHT,
        'websocket': RequestTypes.WEBSOCKET
    }

    

    engine_type = engine_types_map.get(engine, RequestTypes.HTTP)

    asyncio.run(ping_target(uri, engine_type, timeout))
    

async def ping_target(uri: str, engine_type: RequestTypes, timeout: int):

    action_name = f'ping_{uri}'

    timeouts = Timeouts(
        connect_timeout=timeout,
        total_timeout=timeout
    )
    
    selected_engine: T  = engines_registry.get(engine_type, MercuryHTTPClient)(
        concurrency=1,
        timeouts=timeouts,
        reset_connections=False
    )

    action_types = {
        RequestTypes.HTTP: HTTPAction(
            action_name,
            uri
        ),
        RequestTypes.HTTP2: HTTP2Action(
            action_name,
            uri
        ),
        RequestTypes.GRAPHQL: GraphQLAction(
            action_name,
            uri
        ),
        RequestTypes.GRAPHQL_HTTP2: GraphQLHTTP2Action(
            action_name,
            uri
        ),
        RequestTypes.GRPC: GRPCAction(
            action_name,
            uri
        ),
        RequestTypes.PLAYWRIGHT: HTTPAction(
            action_name,
            uri
        ),
        RequestTypes.UDP: UDPAction(
            action_name,
            uri
        ),
        RequestTypes.WEBSOCKET: WebsocketAction(
            action_name,
            uri
        )
    }

    action = action_types.get(
        engine_type, 
        HTTPAction(
            action_name,
            uri
        )
    )

    if engine_type == RequestTypes.PLAYWRIGHT:
        selected_engine = MercuryHTTPClient(timeouts=timeouts)

    try:
        
        action.setup()
        await selected_engine.prepare(action)

    except Exception as ping_error:
        raise ping_error