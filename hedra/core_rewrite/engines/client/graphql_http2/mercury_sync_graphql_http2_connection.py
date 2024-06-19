import asyncio
import time
import uuid
from random import randrange
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import ParseResult, urlparse

import orjson

from hedra.core_rewrite.engines.client.http2 import MercurySyncHTTP2Connection
from hedra.core_rewrite.engines.client.http2.pipe import HTTP2Pipe
from hedra.core_rewrite.engines.client.http2.protocols import HTTP2Connection
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    Cookies,
    HTTPCookie,
    HTTPEncodableValue,
    Metadata,
    URLMetadata,
)
from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.graphql_http2 import (
    GraphQLHTTP2Request,
    GraphQLHTTP2Response,
)

T = TypeVar("T")


def mock_fn():
    return None


try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source = None
    parse = mock_fn
    print_ast = mock_fn


class MercurySyncGraphQLHTTP2Connection(MercurySyncHTTP2Connection):
    def __init__(
        self,
        pool_size: int = 10**3,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        super(MercurySyncGraphQLHTTP2Connection, self).__init__(
            pool_size=pool_size,
            timeouts=timeouts,
            reset_connections=reset_connections,
        )

        self.session_id = str(uuid.uuid4())

    async def query(
        self,
        url: str,
        query: str,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        timeout: Union[Optional[int], Optional[float]] = None,
        redirects: int = 3,
    ) -> GraphQLHTTP2Response:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        GraphQLHTTP2Request(
                            url=url,
                            method="GET",
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            data={
                                "query": query,
                            },
                            redirects=redirects,
                        ),
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return GraphQLHTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="POST",
                    status=408,
                    status_message="Request timed out.",
                )

    async def mutate(
        self,
        url: str,
        query: str,
        operation_name: str = None,
        variables: Dict[str, Any] = None,
        auth: Optional[Tuple[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        headers: Dict[str, str] = {},
        params: Optional[Dict[str, HTTPEncodableValue]] = None,
        timeout: Union[Optional[int], Optional[float]] = None,
        redirects: int = 3,
    ) -> GraphQLHTTP2Response:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        GraphQLHTTP2Request(
                            url=url,
                            method="POST",
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            data={
                                "query": query,
                                "operation_name": operation_name,
                                "variables": variables,
                            },
                            redirects=redirects,
                        ),
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return GraphQLHTTP2Response(
                    metadata=Metadata(),
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="POST",
                    status=408,
                    status_message="Request timed out.",
                )

    async def _request(
        self,
        url: str,
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie]] = None,
        auth: Optional[Tuple[str, str]] = None,
        headers: Optional[Dict[str, str]] = {},
        data: (
            Dict[Literal["query"], str]
            | Dict[Literal["query", "operation_name", "variables"], str]
        ) = None,
        redirects: Optional[int] = 3,
    ):
        timings: Dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = {
            "request_start": None,
            "connect_start": None,
            "connect_end": None,
            "write_start": None,
            "write_end": None,
            "read_start": None,
            "read_end": None,
            "request_end": None,
        }
        timings["request_start"] = time.monotonic()

        result, redirect, timings = await self._execute(
            url,
            method,
            cookies=cookies,
            auth=auth,
            headers=headers,
            data=data,
            timings=timings,
        )

        if redirect:
            location = result.headers.get("location")

            upgrade_ssl = False
            if "https" in location and "https" not in url:
                upgrade_ssl = True

            for _ in range(redirects):
                result, redirect, timings = await self._execute(
                    url,
                    method,
                    cookies=cookies,
                    auth=auth,
                    headers=headers,
                    data=data,
                    timings=timings,
                    upgrade_ssl=upgrade_ssl,
                    redirect_url=location,
                )

                if redirect is False:
                    break

                location = result.headers.get("location")

                upgrade_ssl = False
                if "https" in location and "https" not in url:
                    upgrade_ssl = True

        timings["request_end"] = time.monotonic()
        result.timings.update(timings)

        return result

    async def _execute(
        self,
        request_url: str,
        method: Literal["GET", "POST"],
        cookies: Optional[List[HTTPCookie]] = None,
        auth: Optional[Tuple[str, str]] = None,
        headers: Optional[Dict[str, str]] = {},
        data: (
            Dict[Literal["query"], str]
            | Dict[Literal["query", "operation_name", "variables"], str]
        ) = None,
        upgrade_ssl: bool = False,
        redirect_url: Optional[str] = None,
        timings: Dict[
            Literal[
                "request_start",
                "connect_start",
                "connect_end",
                "write_start",
                "write_end",
                "read_start",
                "read_end",
                "request_end",
            ],
            float | None,
        ] = {},
    ):
        if redirect_url:
            request_url = redirect_url

        connection: HTTP2Connection = None

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (error, connection, pipe, url, upgrade_ssl) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url, ssl_redirect_url=request_url if upgrade_ssl else None
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                (error, connection, pipe, url, _) = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url, ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            if error:
                timings["connect_end"] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        self._concurrency,
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )

                self._pipes.append(HTTP2Pipe(self._max_concurrency))

                return (
                    GraphQLHTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        status_message=str(error),
                        headers={
                            key.encode(): value.encode()
                            for key, value in headers.items()
                        }
                        if headers
                        else {},
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            connection = pipe.send_preamble(connection)

            encoded_headers = self._encode_headers(
                url,
                method,
                data,
                headers=headers,
            )

            connection = pipe.send_request_headers(
                encoded_headers,
                data,
                connection,
            )

            if method == "POST":
                encoded_data = self._encode_data(data)

                connection = await asyncio.wait_for(
                    pipe.submit_request_body(
                        encoded_data,
                        connection,
                    ),
                    timeout=self.timeouts.write_timeout,
                )

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            (status, headers, body, error) = await asyncio.wait_for(
                pipe.receive_response(connection), timeout=self.timeouts.read_timeout
            )

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        self._concurrency,
                        stream_id=randrange(1, 2**20 + 2, 2),
                        reset_connections=self._reset_connections,
                    )
                )
                self._pipes.append(HTTP2Pipe(self._max_concurrency))

                return (
                    GraphQLHTTP2Response(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=status,
                        headers=headers,
                        timings=timings,
                    ),
                    True,
                    timings,
                )

            if error:
                raise error

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get("set-cookie")
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

            self._connections.append(connection)
            self._pipes.append(pipe)

            timings["read_end"] = time.monotonic()

            return (
                GraphQLHTTP2Response(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                    ),
                    cookies=cookies,
                    method=method,
                    status=status,
                    headers=headers,
                    content=body,
                    timings=timings,
                ),
                False,
                timings,
            )

        except Exception as request_exception:
            self._connections.append(
                HTTP2Connection(
                    self._concurrency,
                    stream_id=randrange(1, 2**20 + 2, 2),
                    reset_connections=self._reset_connections,
                )
            )

            self._pipes.append(HTTP2Pipe(self._max_concurrency))

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            timings["read_end"] = time.monotonic()

            return (
                GraphQLHTTP2Response(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                        params=request_url.params,
                        query=request_url.query,
                    ),
                    method=method,
                    status=400,
                    status_message=str(request_exception),
                    timings=timings,
                ),
                False,
                timings,
            )

    def _encode_data(
        self,
        data: (
            Dict[Literal["query"], str]
            | Dict[Literal["query", "operation_name", "variables"], str]
        ) = None,
    ):
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

    def _encode_headers(
        self,
        url: URL,
        method: Literal["GET", "POST"],
        data: (
            Dict[Literal["query"], str]
            | Dict[Literal["query", "operation_name", "variables"], str]
        ) = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        url_path = url.path
        if method == "GET":
            query_string = data.get("query")
            query_string = "".join(query_string.replace("query", "").split())

            url_path += f"?query={{{query_string}}}"

        encoded_headers: List[Tuple[bytes, bytes]] = [
            (b":method", method.encode()),
            (b":authority", url.hostname.encode()),
            (b":scheme", url.scheme.encode()),
            (b":path", url_path.encode()),
            (b"user-agent", b"hedra"),
        ]

        if headers:
            encoded_headers.extend(
                [
                    (k.lower().encode(), v.encode())
                    for k, v in headers.items()
                    if k.lower()
                    not in (
                        "host",
                        "transfer-encoding",
                    )
                ]
            )

        encoded_headers: bytes = self._encoder.encode(encoded_headers)
        encoded_headers: List[bytes] = [
            encoded_headers[i : i + self._settings.max_frame_size]
            for i in range(0, len(encoded_headers), self._settings.max_frame_size)
        ]

        return encoded_headers[0]
