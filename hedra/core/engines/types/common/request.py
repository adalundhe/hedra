from __future__ import annotations
from types import FunctionType
from typing import Coroutine, Dict, Iterator, Union, List
from hedra.core.hooks.types.types import HookType
from .params import Params
from .metadata import Metadata
from .url import URL
from .payload import Payload
from .headers import Headers
from .types import RequestTypes, ProtocolMap
from .hooks import Hooks


class Request:

    def __init__(self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        payload: Union[str, dict, Iterator, bytes, None] = None, 
        user: str=None, tags: List[Dict[str, str]] = [],  
        checks: List[FunctionType] = None, 
        hooks: Dict[str, Coroutine] = {},
        request_type: RequestTypes = RequestTypes.HTTP
    ) -> None:

        self.protocols = ProtocolMap()
        self.name = name
        self.method = method
        self.type = request_type

        address_family, protocol = self.protocols[request_type]
        self.url = URL(url, family=address_family, protocol=protocol)
        self.params = Params(params)
        self.headers = Headers(headers)
        self.payload = Payload(payload)
        self.metadata = Metadata(user, tags)
        self.ssl_context = None
        self.is_setup = False
        self.checks = checks
        self.hooks = Hooks(**hooks)

    def __aiter__(self):
        return self.payload.__aiter__()

    def setup_http_request(self):
        self.payload.setup_payload()

        if self.payload.has_data:
            self.headers['Content-Length'] = str(self.payload.size)

        self.headers.setup_http_headers(self.method, self.url, self.params)
        self.is_setup = True


    def setup_http2_request(self):
        self.payload.setup_payload()

        if self.payload.has_data:
            self.headers['Content-Length'] = str(self.payload.size)
 
        self.headers.setup_http2_headers(self.method, self.url)
        self.is_setup = True

    def setup_websocket_request(self):
        self.payload.setup_payload()

        if self.payload.has_data:
            self.headers['Content-Length'] = str(self.payload.size)

        self.headers.setup_websocket_headders(self.method, self.url)
        self.is_setup = True

    def setup_graphql_request(self, use_http2=False):
        self.method = 'POST'
        self.payload.setup_graphql_query()
        self.headers.setup_graphql_headers(self.url, self.params, use_http2=use_http2)
        self.is_setup = True

    def setup_grpc_request(self, grpc_request_timeout=60):
        self.method = 'POST'
        self.payload.setup_grpc_protobuf()
        self.headers.setup_grpc_headers(
            self.url, 
            timeout=grpc_request_timeout
        )

        self.is_setup = True