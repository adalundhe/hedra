from __future__ import annotations

from hedra.core_rewrite.engines.client.http2.models.http2 import HTTP2Response


class GraphQLHTTP2Response(HTTP2Response):
    class Config:
        arbitrary_types_allowed = True
