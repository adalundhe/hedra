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
    GraphQLHTTP2ActionParser,
    GRPCActionParser,
    HTTPActionParser,
    HTTP2ActionParser,
    HTTP3ActionParser,
    PlaywrightActionParser,
    UDPActionParser,
    WebsocketActionParser
)


class Parser:

    def __init__(
        self,
        config: Config
    ) -> None:
        
        self.config = config
        self._parsers: Dict[
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

        self._active_parsers: Dict[
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

    async def parse(
        self, 
        action_data: Dict[str, Any],
        stage: str,
        options: Dict[str, Any]={}
    ):
        engine_type = action_data.get('engine')

        parser = self._active_parsers.get(engine_type)
        if parser is None:
            parser = self._parsers.get(engine_type)(
                self.config,
                options
            )

        return await parser.parse(
            action_data,
            stage
        )