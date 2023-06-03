from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from typing import (
    Dict,
    Any,
    Union,
    Callable
)
from .parser_types import (
    GraphQLActionParser,
    GraphQLResultParser,
    GraphQLHTTP2ActionParser,
    GraphQLHTTP2ResultParser,
    GRPCActionParser,
    GRPCResultParser,
    HTTPActionParser,
    HTTPResultParser,
    HTTP2ActionParser,
    HTTP2ResultParser,
    HTTP3ActionParser,
    HTTP3ResultParser,
    PlaywrightActionParser,
    PlaywrightResultParser,
    UDPActionParser,
    UDPResultParser,
    WebsocketActionParser,
    WebsocketResultParser
)


class Parser:

    def __init__(self) -> None:
        
        self._action_parsers: Dict[
            str,
            Callable[
                [
                    Dict[str, Any]
                ],
                Union[
                    GraphQLActionParser,
                    GraphQLHTTP2ActionParser,
                    GRPCActionParser,
                    HTTPActionParser,
                    HTTP2ActionParser,
                    HTTP3ActionParser,
                    PlaywrightActionParser,
                    UDPActionParser,
                    WebsocketActionParser
                ]
            ]
        ] = {
            'graphql': lambda config, options: GraphQLActionParser(
                config,
                options
            ),
            'graphqlh2': lambda config, options: GraphQLHTTP2ActionParser(
                config,
                options
            ),
            'grpc': lambda config, options: GRPCActionParser(
                config,
                options
            ),
            'http': lambda config, options: HTTPActionParser(
                config,
                options
            ),
            'http2': lambda config, options: HTTP2ActionParser(
                config,
                options
            ),
            'http3': lambda config, options: HTTP3ActionParser(
                config,
                options
            ),
            'playwright': lambda config, options: PlaywrightActionParser(
                config,
                options
            ),
            'udp': lambda config, options: UDPActionParser(
                config,
                options
            ),
            'websocket': lambda config, options: WebsocketActionParser(
                config,
                options
            )
        }

        self._result_parsers: Dict[
            str,
            Callable[
                [
                    Dict[str, Any]
                ],
                Union[
                    GraphQLResultParser,
                    GraphQLHTTP2ResultParser,
                    GRPCResultParser,
                    HTTPResultParser,
                    HTTP2ResultParser,
                    HTTP3ResultParser,
                    PlaywrightResultParser,
                    UDPResultParser,
                    WebsocketResultParser
                ]
            ]
        ] = {
            'graphql': lambda config, options: GraphQLResultParser(
                config,
                options
            ),
            'graphqlh2': lambda config, options: GraphQLHTTP2ResultParser(
                config,
                options
            ),
            'grpc': lambda config, options: GRPCResultParser(
                config,
                options
            ),
            'http': lambda config, options: HTTPResultParser(
                config,
                options
            ),
            'http2': lambda config, options: HTTP2ResultParser(
                config,
                options
            ),
            'http3': lambda config, options: HTTP3ResultParser(
                config,
                options
            ),
            'playwright': lambda config, options: PlaywrightResultParser(
                config,
                options
            ),
            'udp': lambda config, options: UDPResultParser(
                config,
                options
            ),
            'websocket': lambda config, options: WebsocketResultParser(
                config,
                options
            )
        }

        self._active_action_parsers: Dict[
            str, 
            Union[
                GraphQLActionParser,
                GraphQLHTTP2ActionParser,
                GRPCActionParser,
                HTTPActionParser,
                HTTP2ActionParser,
                HTTP3ActionParser,
                PlaywrightActionParser,
                UDPActionParser,
                WebsocketActionParser
            ]
        ] = {}

        self._active_result_parser: Dict[
            str,
            Union[
                GraphQLResultParser,
                GraphQLHTTP2ResultParser,
                GRPCResultParser,
                HTTPResultParser,
                HTTP2ResultParser,
                HTTP3ResultParser,
                PlaywrightResultParser,
                UDPResultParser,
                WebsocketResultParser
            ]
        ] = {}

    async def parse_action(
        self, 
        action_data: Dict[str, Any],
        stage: str,
        config: Config,
        options: Dict[str, Any]={}
    ):
        engine_type = action_data.get('engine')
        parser = self._active_action_parsers.get(engine_type)

        if parser is None:
            parser = self._action_parsers.get(engine_type)(
                config,
                options
            )

        return await parser.parse(
            action_data,
            stage
        )
    
    async def parse_result(
        self,
        result_data: Dict[str, Any],
        config: Config,
        options: Dict[str, Any]={}
    ):
        
        engine_type = result_data.get('engine')
        parser = self._active_result_parser.get(engine_type)
        if parser is None:
            parser = self._result_parsers.get(engine_type)(
                config,
                options
            )

        return await parser.parse(result_data) 