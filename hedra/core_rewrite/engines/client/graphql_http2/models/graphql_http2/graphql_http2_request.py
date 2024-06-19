from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
)
from urllib.parse import urlparse

import orjson

from hedra.core_rewrite.engines.client.http2.models.http2 import HTTP2Request
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
)


def mock_fn():
    return None


try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source = None
    parse = mock_fn
    print_ast = mock_fn


class GraphQLHTTP2Request(HTTP2Request):
    data: (
        Dict[Literal["query"], str]
        | Dict[Literal["query", "operation_name", "variables"], str]
    )

    class Config:
        arbitrary_types_allowed = True

    def parse_url(self):
        return urlparse(self.url)

    def encode_headers(self, url: URL) -> List[Tuple[bytes, bytes]]:
        url_path = url.path

        if self.method == "GET":
            data: Dict[Literal["query", "operation_name", "variables"], str] = self.data
            query_string = data.get("query")
            query_string = "".join(query_string.replace("query", "").split())

            url_path += f"?query={{{query_string}}}"

        encoded_headers = [
            (b":method", self.method.encode()),
            (b":authority", url.hostname.encode()),
            (b":scheme", url.scheme.encode()),
            (b":path", url_path.encode()),
        ]

        encoded_headers.extend(
            [
                (k.lower().encode(), v.encode())
                for k, v in self.headers.items()
                if k.lower()
                not in (
                    b"host",
                    b"transfer-encoding",
                )
            ]
        )

        return encoded_headers

    def encode_data(self):
        encoded_data: Optional[bytes] = None

        if self.method == "POST":
            data: Dict[Literal["query", "operation_name", "variables"], str] = self.data

            source = Source(data.get("query"))
            document_node = parse(source)
            query_string = print_ast(document_node)

            query = {"query": query_string}

            operation_name = data.get("operation_name")
            variables = data.get("variables")

            if operation_name:
                query["operationName"] = operation_name

            if variables:
                query["variables"] = variables

            encoded_data = orjson.dumps(query)

        return encoded_data
