from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
)

import orjson
from pydantic import BaseModel, StrictInt, StrictStr

from hedra.core_rewrite.engines.client.shared.models import URL, HTTPCookie
from hedra.core_rewrite.engines.client.shared.protocols import NEW_LINE


def mock_fn():
    return None


try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source = None
    parse = mock_fn
    print_ast = mock_fn


class GraphQLRequest(BaseModel):
    url: StrictStr
    method: Literal[
        "GET",
        "POST",
    ]
    cookies: Optional[List[HTTPCookie]] = None
    auth: Optional[Tuple[str, str]] = None
    cookies: Optional[List[HTTPCookie]] = None
    headers: Dict[str, str] = {}
    data: (
        Dict[Literal["query"], str]
        | Dict[Literal["query", "operation_name", "variables"], str]
    )
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True

    def prepare(self, url: URL):
        url_path = url.path
        if self.method == "GET":
            query_string = self.data.get("query")
            query_string = "".join(query_string.replace("query", "").split())

            url_path += f"?query={{{query_string}}}"

        get_base = f"{self.method} {url_path} HTTP/1.1{NEW_LINE}"

        port = url.port or (443 if url.scheme == "https" else 80)

        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        data: Optional[bytes] = None
        size: int = 0

        if self.method == "POST":
            source = Source(self.data.get("query"))
            document_node = parse(source)
            query_string = print_ast(document_node)

            query = {"query": query_string}

            operation_name = self.data.get("operation_name")
            variables = self.data.get("variables")

            if operation_name:
                query["operationName"] = operation_name

            if variables:
                query["variables"] = variables

            data = orjson.dumps(query)
            size = len(data)

        header_items = []

        if size > 0:
            header_items = [
                ("Content-Length", size),
            ]

        header_items.extend(self.headers.items())

        for key, value in header_items:
            get_base += f"{key}: {value}{NEW_LINE}"

        if self.cookies:
            cookies = []

            for cookie_data in self.cookies:
                if len(cookie_data) == 1:
                    cookies.append(cookie_data[0])

                elif len(cookie_data) == 2:
                    cookie_name, cookie_value = cookie_data
                    cookies.append(f"{cookie_name}={cookie_value}")

            cookies = "; ".join(cookies)
            get_base += f"cookie: {cookies}{NEW_LINE}"

        return (get_base + NEW_LINE).encode(), data
