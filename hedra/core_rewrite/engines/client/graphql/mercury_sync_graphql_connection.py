import asyncio
import time
import uuid
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import (
    ParseResult,
    urlparse,
)

import orjson

from hedra.core_rewrite.engines.client.http import MercurySyncHTTPConnection
from hedra.core_rewrite.engines.client.http.protocols import HTTPConnection
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    Cookies,
    HTTPCookie,
    HTTPEncodableValue,
    Metadata,
    URLMetadata,
)
from hedra.core_rewrite.engines.client.shared.protocols import NEW_LINE
from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.graphql import (
    GraphQLResponse,
)


def mock_fn():
    return None


try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source = None
    parse = mock_fn
    print_ast = mock_fn


class MercurySyncGraphQLConnection(MercurySyncHTTPConnection):
    def __init__(
        self,
        pool_size: int = 10**3,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ) -> None:
        super(MercurySyncGraphQLConnection, self).__init__(
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
    ) -> GraphQLResponse:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
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
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return GraphQLResponse(
                    metadata=Metadata(),
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query,
                    ),
                    headers=headers,
                    method="GET",
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
    ) -> GraphQLResponse:
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        url=url,
                        method="POST",
                        cookies=cookies,
                        auth=auth,
                        headers=headers,
                        params=params,
                        data={
                            "query": query,
                            "operation_name": operation_name,
                            "variables": variables,
                        },
                        redirects=redirects,
                    ),
                    timeout=timeout,
                )

            except asyncio.TimeoutError:
                url_data = urlparse(url)

                return GraphQLResponse(
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
            location = result.headers.get(b"location").decode()

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
                    upgrade_ssl=upgrade_ssl,
                    redirect_url=location,
                    timings=timings,
                )

                if redirect is False:
                    break

                location = result.headers.get(b"location").decode()

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
    ) -> Tuple[
        GraphQLResponse,
        bool,
        Dict[
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
        ],
    ]:
        if redirect_url:
            request_url = redirect_url

        try:
            if timings["connect_start"] is None:
                timings["connect_start"] = time.monotonic()

            (connection, url, upgrade_ssl) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url, ssl_redirect_url=request_url if upgrade_ssl else None
                ),
                timeout=self.timeouts.connect_timeout,
            )

            if upgrade_ssl:
                ssl_redirect_url = request_url.replace("http://", "https://")

                connection, url, _ = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url, ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout,
                )

                request_url = ssl_redirect_url

            if connection.reader is None:
                timings["connect_end"] = time.monotonic()
                self._connections.append(
                    HTTPConnection(
                        reset_connections=self.reset_connections,
                    )
                )

                return (
                    GraphQLResponse(
                        url=URLMetadata(
                            host=url.hostname,
                            path=url.path,
                        ),
                        method=method,
                        status=400,
                        headers=headers,
                        timings=timings,
                    ),
                    False,
                    timings,
                )

            timings["connect_end"] = time.monotonic()

            if timings["write_start"] is None:
                timings["write_start"] = time.monotonic()

            if method == "GET":
                encoded_headers = self.encode_headers(
                    url,
                    method,
                    data,
                    headers=headers,
                    cookies=cookies,
                )

                connection.write(encoded_headers)

            else:
                encoded_data = self.encode_data(data)

                encoded_headers = self.encode_headers(
                    url,
                    method,
                    data,
                    headers=headers,
                    cookies=cookies,
                    encoded_data=encoded_data,
                )

                connection.write(encoded_headers)
                connection.write(encoded_data)

            timings["write_end"] = time.monotonic()

            if timings["read_start"] is None:
                timings["read_start"] = time.monotonic()

            response_code = await asyncio.wait_for(
                connection.reader.readline(), timeout=self.timeouts.read_timeout
            )

            headers: Dict[bytes, bytes] = await asyncio.wait_for(
                connection.read_headers(), timeout=self.timeouts.read_timeout
            )

            status_string: List[bytes] = response_code.split()
            status = int(status_string[1])

            if status >= 300 and status < 400:
                timings["read_end"] = time.monotonic()
                self._connections.append(connection)

                return (
                    GraphQLResponse(
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

            content_length = headers.get(b"content-length")
            transfer_encoding = headers.get(b"transfer-encoding")

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b"set-cookie")
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

            # We require Content-Length or Transfer-Encoding headers to read a
            # request body, otherwise it's anyone's guess as to how big the body
            # is, and we ain't playing that game.

            if content_length:
                body = await asyncio.wait_for(
                    connection.readexactly(int(content_length)),
                    timeout=self.timeouts.read_timeout,
                )

            elif transfer_encoding:
                body = bytearray()
                all_chunks_read = False

                while True and not all_chunks_read:
                    chunk_size = int(
                        (
                            await asyncio.wait_for(
                                connection.readline(),
                                timeout=self.timeouts.read_timeout,
                            )
                        ).rstrip(),
                        16,
                    )

                    if not chunk_size:
                        # read last CRLF
                        await asyncio.wait_for(
                            connection.readline(), timeout=self.timeouts.read_timeout
                        )
                        break

                    chunk = await asyncio.wait_for(
                        connection.readexactly(chunk_size + 2),
                        self.timeouts.read_timeout,
                    )
                    body.extend(chunk[:-2])

                all_chunks_read = True

            self._connections.append(connection)

            timings["read_end"] = time.monotonic()

            return (
                GraphQLResponse(
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
                HTTPConnection(reset_connections=self.reset_connections)
            )

            if isinstance(request_url, str):
                request_url: ParseResult = urlparse(request_url)

            timings["read_end"] = time.monotonic()

            return (
                GraphQLResponse(
                    url=URLMetadata(
                        host=request_url.hostname,
                        path=request_url.path,
                    ),
                    method=method,
                    status=400,
                    status_message=str(request_exception),
                    timings=timings,
                ),
                False,
                timings,
            )

    def encode_data(
        self,
        data: (
            Dict[Literal["query"], str]
            | Dict[Literal["query", "operation_name", "variables"], str]
        ),
    ):
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

        return orjson.dumps(query)

    def encode_headers(
        self,
        url: URL,
        method: Literal["GET", "POST"],
        data: (
            Dict[Literal["query"], str]
            | Dict[Literal["query", "operation_name", "variables"], str]
        ),
        headers: Optional[Dict[str, str]] = None,
        cookies: Optional[List[HTTPCookie]] = None,
        encoded_data: Optional[bytes] = None,
    ):
        url_path = url.path

        if method == "GET":
            query_string = data.get("query")
            query_string = "".join(query_string.replace("query", "").split())

            url_path += f"?query={{{query_string}}}"

        get_base = f"{method} {url_path} HTTP/1.1{NEW_LINE}"

        port = url.port or (443 if url.scheme == "https" else 80)

        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        header_items = {
            "user-agent": "hedra",
        }

        if method == "POST" and encoded_data:
            header_items["content-length"] = len(encoded_data)
            header_items["content-type"] = "application/json"

        if headers:
            header_items.update(headers)

        for key, value in header_items.items():
            get_base += f"{key}: {value}{NEW_LINE}"

        if cookies:
            cookies = []

            for cookie_data in cookies:
                if len(cookie_data) == 1:
                    cookies.append(cookie_data[0])

                elif len(cookie_data) == 2:
                    cookie_name, cookie_value = cookie_data
                    cookies.append(f"{cookie_name}={cookie_value}")

            cookies = "; ".join(cookies)
            get_base += f"cookie: {cookies}{NEW_LINE}"

        return (get_base + NEW_LINE).encode(), data
