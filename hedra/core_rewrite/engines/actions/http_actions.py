from ssl import SSLContext
from hedra.core_rewrite.hooks.resolved_arg import (
    ResolvedArg,
    ResolvedAuth,
    ResolvedData,
    ResolvedHeaders,
    ResolvedParams,
    ResolvedQuery,
    ResolvedURL
)
from hedra.core_rewrite.engines.client.client_types.common import URL
from typing import Dict, Any, Tuple, Optional


class HTTPAction:

    def __init__(
        self,
        socket_config: Optional[Tuple[Any, ...]]=None,
        ssl_context: Optional[SSLContext]=None,
        encoded_headers: Optional[bytes]=None,
        encoded_data: Optional[bytes]=None,
        is_stream: bool=False
    ) -> None:
        self.socket_config = socket_config
        self.ssl_context = ssl_context
        self.encoded_headers = encoded_headers
        self.encoded_data = encoded_data
        self.is_stream = is_stream

    @property
    def to_data(self):
        return (
            self.socket_config,
            self.ssl_context,
            self.encoded_headers,
            self.encoded_data,
            self.is_stream
        )

    @classmethod
    def from_resolved_args(
        self,
        resolved: Dict[str, ResolvedArg]
    ):
        
        args: Dict[str, Any] = {}
        url_arg: ResolvedArg | None = resolved.get('url')
        if url_arg:
            args['socket_config'] = url_arg.data.url.socket_config
            args['ssl_context'] = url_arg.data.ssl_context

        headers_arg: ResolvedArg | None = resolved.get('headers')
        if headers_arg:
            args['encoded_headers'] = headers_arg.data.headers

        data_arg: ResolvedArg | None = resolved.get('data')
        if data_arg:
            args['encoded_data'] = data_arg.data.data
            args['is_stream'] = data_arg.data.is_stream


        return HTTPAction(**args)
