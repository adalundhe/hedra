from hedra.execution.events.handlers.responses.types.fast_http_response import FastHttpResponse
from hedra.execution.engines.types.fast_http_engine import FastHttpEngine
from .types import (
    HttpResponse,
    PlaywrightResponse,
    CustomResponse,
    WebsocketResponse,
    GrpcResponse,
    GraphQLResponse
)


class Response:

    def __init__(self, action):
        response_type = {
            'http': HttpResponse,
            'fast-http': FastHttpResponse,
            'playwright': PlaywrightResponse,
            'custom': CustomResponse,
            'websocket': WebsocketResponse,
            'grpc': GrpcResponse,
            'graphql': GraphQLResponse
        }
        self._response = response_type.get(
            action.get('action_type'),
            CustomResponse
        )(action)

    async def assert_response(self):
        await self._response.assert_response()

    async def to_dict(self):
        return self._response.to_dict()