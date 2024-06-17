import asyncio
import binascii
from collections import defaultdict
from ssl import SSLContext
from typing import (
    Any,
    Callable,
    Coroutine,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)
from urllib.parse import urlencode

import orjson
from google.protobuf.message import Message
from graphql import Source, parse, print_ast

from hedra.core_rewrite.engines.client.graphql import MercurySyncGraphQLConnection
from hedra.core_rewrite.engines.client.graphql_http2 import (
    MercurySyncGraphQLHTTP2Connection,
)
from hedra.core_rewrite.engines.client.grpc import MercurySyncGRPCConnection
from hedra.core_rewrite.engines.client.http import MercurySyncHTTPConnection
from hedra.core_rewrite.engines.client.http2 import MercurySyncHTTP2Connection
from hedra.core_rewrite.engines.client.http2.fast_hpack import Encoder
from hedra.core_rewrite.engines.client.http2.settings import Settings
from hedra.core_rewrite.engines.client.http3 import MercurySyncHTTP3Connection
from hedra.core_rewrite.engines.client.http3.protocols.quic_protocol import (
    FrameType,
    encode_frame,
)
from hedra.core_rewrite.engines.client.shared.models import URL, RequestType
from hedra.core_rewrite.engines.client.shared.protocols import (
    NEW_LINE,
    WEBSOCKETS_VERSION,
)
from hedra.core_rewrite.engines.client.shared.request_types_map import RequestTypesMap
from hedra.core_rewrite.engines.client.udp import MercurySyncUDPConnection
from hedra.core_rewrite.engines.client.websocket import MercurySyncWebsocketConnection
from hedra.core_rewrite.engines.client.websocket.models.websocket.utils import (
    create_sec_websocket_key,
    pack_hostname,
)

from .call_arg import CallArg
from .resolved_arg import ResolvedArg
from .resolved_arg_type import ResolvedArgType
from .resolved_auth import ResolvedAuth
from .resolved_cookies import ResolvedCookies
from .resolved_data import ResolvedData
from .resolved_headers import ResolvedHeaders
from .resolved_method import ResolvedMethod
from .resolved_params import ResolvedParams
from .resolved_url import ResolvedURL


class CallResolver:
    def __init__(self) -> None:
        self._args: Dict[int, List[CallArg]] = defaultdict(list)
        self._kwargs: Dict[int, List[CallArg]] = defaultdict(list)

        self._position_map: Dict[str, Dict[int, str]] = {
            "graphql": {0: "url", 1: "query"},
            "graphqlh2": {0: "url", 1: "query"},
            "grpc": {0: "url"},
            "http": {0: "url"},
            "http2": {0: "url"},
            "http3": {0: "url"},
            "udp": {0: "url"},
            "websocket": {0: "url"},
        }

        self._call_optimization_map: Dict[
            str, Callable[[CallArg], Coroutine[Any, Any, None]]
        ] = {
            "url": self._set_url,
            "method": self._set_method,
            "auth": self._set_auth,
            "cookies": self._set_cookies,
            "params": self._set_params,
            "headers": self._set_headers,
            "data": self._set_data,
        }

        self._request_types_map = RequestTypesMap()
        self._resolved: Dict[str, Dict[str, ResolvedArg]] = defaultdict(dict)

        self._connections: Dict[
            RequestType,
            Callable[
                [],
                Union[
                    MercurySyncGraphQLConnection,
                    MercurySyncGraphQLHTTP2Connection,
                    MercurySyncGRPCConnection,
                    MercurySyncHTTPConnection,
                    MercurySyncHTTP2Connection,
                    MercurySyncHTTP3Connection,
                    MercurySyncUDPConnection,
                    MercurySyncWebsocketConnection,
                ],
            ],
        ] = {
            RequestType.GRAPHQL: lambda: MercurySyncGraphQLConnection(pool_size=1),
            RequestType.GRAPHQL_HTTP2: lambda: MercurySyncGraphQLHTTP2Connection(
                pool_size=1
            ),
            RequestType.GRPC: lambda: MercurySyncGRPCConnection(pool_size=1),
            RequestType.HTTP: lambda: MercurySyncHTTPConnection(pool_size=1),
            RequestType.HTTP2: lambda: MercurySyncHTTP2Connection(pool_size=1),
            RequestType.HTTP3: lambda: MercurySyncHTTP3Connection(pool_size=1),
            RequestType.UDP: lambda: MercurySyncUDPConnection(pool_size=1),
            RequestType.WEBSOCKET: lambda: MercurySyncWebsocketConnection(pool_size=1),
        }

        self._call_engine_types: Dict[str, str] = {}
        self._call_workflows: Dict[str, str] = {}
        self._call_args: Dict[
            Literal[
                "method",
                "url",
                "data",
                "params",
                "headers",
                "auth",
                "cookies",
            ],
            List[Tuple[CallArg, Callable[..., None]]],
        ] = defaultdict(list)

        self._available_optimizations: Dict[
            Literal[
                "method",
                "url",
                "data",
                "params",
                "headers",
                "auth",
                "cookies",
            ],
            List[str],
        ] = defaultdict(list)

    def __iter__(self):
        for call_id, args in self._resolved.items():
            yield call_id, args

    def __getitem__(self, call_id: str):
        return self._resolved.get(call_id)

    def get_engine(self, call_id: str):
        return self._call_engine_types.get(call_id)

    def get_workflow(self, call_id: str):
        return self._call_workflows.get(call_id)

    def add_args(self, optimizer_args: Dict[str, List[CallArg]]):
        for call_id in optimizer_args:
            self._args[call_id].extend(
                [arg for arg in optimizer_args[call_id] if arg.arg_type == "arg"]
            )

            self._kwargs[call_id].extend(
                [arg for arg in optimizer_args[call_id] if arg.arg_type == "kwarg"]
            )

    async def resolve_arg_types(self):
        await asyncio.gather(
            *[
                self._optimize_engine_type(engine_type)
                for engine_type in self._position_map
            ]
        )

        call_ids = set(self._args)

        for call_id in call_ids:
            call_engine_type = self.get_engine(call_id)
            call_workflow = self.get_workflow(call_id)

            if call_workflow:
                arg_set: Dict[
                    Literal[
                        "method",
                        "url",
                        "data",
                        "params",
                        "headers",
                        "auth",
                        "cookies",
                    ],
                    ResolvedArg[
                        ResolvedURL
                        | ResolvedHeaders
                        | ResolvedAuth
                        | ResolvedParams
                        | ResolvedData
                    ],
                ] = self._resolved[call_id]

    async def _optimize_engine_type(self, engine_type: str):
        engine_type_args: List[Tuple[CallArg, Callable[..., None]]] = []
        engine_arg_positions = self._position_map.get(engine_type)

        for call_id in self._args:
            for arg in self._args[call_id]:
                self._call_engine_types[call_id] = self._request_types_map[arg.engine]
                self._call_workflows[call_id] = arg.workflow
                self._resolved[call_id]["method"] = ResolvedArg(
                    ResolvedArgType.METHOD,
                    arg,
                    ResolvedMethod(method=arg.method),
                )

        for call_id in self._args:
            engine_type_args.extend(
                [
                    (
                        arg,
                        self._call_optimization_map.get(
                            engine_arg_positions.get(arg.position)
                        ),
                    )
                    for arg in self._args[call_id]
                    if arg.engine == engine_type
                ]
            )

        for call_id in self._kwargs:
            engine_type_args.extend(
                [
                    (
                        arg,
                        self._call_optimization_map.get(arg.arg_name),
                    )
                    for arg in self._kwargs[call_id]
                    if arg.engine == engine_type
                ]
            )

        for arg, optimizer in engine_type_args:
            arg_type = arg.arg_name
            if arg_type is None:
                arg_type = engine_arg_positions.get(arg.position)

            if arg_type == "query" and arg.method == "query":
                arg_type = "params"
                optimizer = self._set_params

            elif arg_type == "query" and arg.method == "mutate":
                arg_type = "data"
                optimizer = self._set_data

            self._available_optimizations[arg_type].append(arg.call_id)
            self._call_args[arg_type].append((arg, optimizer))

        await asyncio.gather(
            *[call_optimizer(arg) for arg, call_optimizer in self._call_args["url"]]
        )

        for arg, call_optimizer in self._call_args["method"]:
            call_optimizer(arg)

        for arg, call_optimizer in self._call_args["data"]:
            call_optimizer(arg)

        for arg, call_optimizer in self._call_args["params"]:
            call_optimizer(arg)

        for arg, call_optimizer in self._call_args["auth"]:
            call_optimizer(arg)

        for arg, call_optimizer in self._call_args["cookies"]:
            call_optimizer(arg)

        for arg, call_optimizer in self._call_args["headers"]:
            call_optimizer(arg)

    async def _set_url(self, arg: CallArg):
        request_type = self._request_types_map[arg.engine]

        url_string: str = arg.value
        connection = self._connections.get(request_type)()
        connection = self._connections.get(request_type)()

        ssl_context: Union[SSLContext, None] = None
        upgrade_ssl = False

        url, upgrade_ssl = await self._connect_to_url(
            connection, url_string, upgrade_ssl
        )

        if upgrade_ssl:
            url, _ = await self._connect_to_url(connection, url_string, upgrade_ssl)

        self._resolved[arg.call_id]["url"] = ResolvedArg(
            arg_type=ResolvedArgType.URL,
            call_arg=arg,
            value=ResolvedURL(ssl_context=ssl_context, url=url),
        )

    async def _connect_to_url(
        self,
        connection: MercurySyncGraphQLConnection
        | MercurySyncGRPCConnection
        | MercurySyncGraphQLHTTP2Connection
        | MercurySyncGraphQLHTTP2Connection
        | MercurySyncHTTPConnection
        | MercurySyncHTTP3Connection
        | MercurySyncUDPConnection
        | MercurySyncWebsocketConnection,
        url_string: str,
        upgrade_ssl: bool = False,
    ) -> Tuple[URL, bool]:
        if isinstance(
            connection,
            (
                MercurySyncGraphQLConnection,
                MercurySyncHTTPConnection,
                MercurySyncHTTP3Connection,
                MercurySyncWebsocketConnection,
            ),
        ):
            (active_connection, url, upgrade_ssl) = await asyncio.wait_for(
                connection._connect_to_url_location(
                    url_string,
                    ssl_redirect_url=url_string if upgrade_ssl else None,
                ),
                timeout=connection.timeouts.connect_timeout,
            )

            connection._connections.append(active_connection)

            return (
                url,
                upgrade_ssl,
            )

        elif isinstance(connection, MercurySyncUDPConnection):
            (_, active_connection, url) = await asyncio.wait_for(
                connection._connect_to_url_location(
                    url_string,
                    ssl_redirect_url=url_string if upgrade_ssl else None,
                ),
                timeout=connection.timeouts.connect_timeout,
            )

            connection._connections.append(active_connection)

            return (
                url,
                upgrade_ssl,
            )

        else:
            (_, active_connection, pipe, url, upgrade_ssl) = await asyncio.wait_for(
                connection._connect_to_url_location(
                    url_string, ssl_redirect_url=url_string if upgrade_ssl else None
                ),
                timeout=connection.timeouts.connect_timeout,
            )

            connection._connections.append(active_connection)
            connection._pipes.append(pipe)

            return (
                url,
                upgrade_ssl,
            )

    def _set_headers(self, arg: CallArg):
        request_type = self._request_types_map[arg.engine]

        match request_type:
            case RequestType.GRAPHQL | RequestType.HTTP:
                self._parse_to_http_headers(arg)

            case RequestType.GRAPHQL_HTTP2 | RequestType.GRPC | RequestType.HTTP2:
                self._parse_to_http2_headers(arg)

            case RequestType.HTTP3:
                self._parse_to_http3_headers(arg)

            case RequestType.PLAYWRIGHT | RequestType.UDP:
                pass

            case RequestType.WEBSOCKET:
                self._parse_to_websocket_headers(arg)

            case _:
                raise Exception("Err. - Invalid request type.")

    def _set_cookies(self, arg: CallArg):
        request_type = self._request_types_map[arg.engine]

        match request_type:
            case RequestType.GRAPHQL | RequestType.HTTP | RequestType.WEBSOCKET:
                self._parse_to_http_cookies(arg)

            case RequestType.GRAPHQL_HTTP2 | RequestType.GRPC | RequestType.HTTP2:
                self._parse_to_http2_or_http3_cookies(arg)

            case RequestType.HTTP3:
                self._parse_to_http2_or_http3_cookies(arg)

            case RequestType.PLAYWRIGHT | RequestType.UDP:
                pass

            case _:
                raise Exception("Err. - Invalid request type.")

    def _set_method(self, arg: CallArg):
        self._resolved[arg.call_id]["method"] = ResolvedArg(
            arg_type=ResolvedArgType.METHOD,
            call_arg=arg,
            value=ResolvedMethod(method=arg.value),
        )

    def _set_auth(self, arg: CallArg):
        auth_params: Tuple[str] | Tuple[str, str] = arg.value
        params_count = len(auth_params)

        if params_count == 1:
            username = auth_params[0]
            self._resolved[arg.call_id]["auth"] = ResolvedArg(
                arg_type=ResolvedArgType.AUTH,
                call_arg=arg,
                value=ResolvedAuth(
                    username=username,
                    auth=f"{username}:",
                ),
            )

        elif params_count == 2:
            username, password = auth_params
            self._resolved[arg.call_id]["auth"] = ResolvedArg(
                arg_type=ResolvedArgType.AUTH,
                call_arg=arg,
                value=ResolvedAuth(
                    username=username,
                    password=password,
                    auth=f"{username}:{password}",
                ),
            )

        else:
            raise Exception(
                "Err. - can only except username tuple of length one or username/password tuple of length two."
            )

    def _set_params(self, arg: CallArg):
        request_type = self._request_types_map[arg.engine]

        match request_type:
            case RequestType.GRAPHQL | RequestType.GRAPHQL_HTTP2:
                self._parse_to_graphql_params(arg)

            case _:
                self._parse_to_http_params(arg)

    def _set_data(self, arg: CallArg):
        request_type = self._request_types_map[arg.engine]

        match request_type:
            case RequestType.GRAPHQL | RequestType.GRAPHQL_HTTP2:
                self._parse_to_graphql_data(arg)

            case RequestType.GRPC:
                self._parse_to_grpc_data(arg)

            case (
                RequestType.HTTP
                | RequestType.HTTP2
                | RequestType.UDP
                | RequestType.WEBSOCKET
            ):
                self._parse_to_http_or_udp_data(arg)

            case RequestType.HTTP3:
                self._parse_to_http3_data(arg)

            case RequestType.PLAYWRIGHT:
                pass

            case _:
                raise Exception("Err. - Invalid request type.")

    def _parse_to_graphql_params(self, arg: CallArg):
        query_string: str = arg.value
        query_string = "".join(query_string.split()).replace("query", "", 1)

        query_url = f"?query={{{query_string}}}"

        self._resolved[arg.call_id]["params"] = ResolvedArg(
            arg_type=ResolvedArgType.PARAMS,
            call_arg=arg,
            value=ResolvedParams(params=query_url),
        )

        has_headers = arg.call_id in self._available_optimizations["headers"]

        if has_headers is False:
            self._call_args["headers"].append(
                (
                    CallArg(
                        call_name=arg.call_name,
                        call_id=arg.call_id,
                        arg_name="headers",
                        arg_type="kwarg",
                        workflow=arg.workflow,
                        engine=arg.engine,
                        method=arg.method,
                        value={},
                        data_type="static",
                    ),
                    self._set_headers,
                )
            )

            self._available_optimizations["headers"].append(arg.call_id)

    def _parse_to_http_params(self, arg: CallArg):
        params_dict: Dict[str, Any] = arg.value
        params = urlencode(params_dict)

        self._resolved[arg.call_id]["params"] = ResolvedArg(
            arg_type=ResolvedArgType.PARAMS,
            call_arg=arg,
            value=ResolvedParams(params=f"?{params}"),
        )

    def _parse_to_http_headers(self, arg: CallArg):
        resolved_url: ResolvedURL = self._resolved[arg.call_id]["url"].value
        url_path = resolved_url.url.path

        optimized_params: Optional[ResolvedArg[ResolvedParams]] = self._resolved[
            arg.call_id
        ].get("params")

        if optimized_params:
            url_path += optimized_params.value.params

        method = arg.method

        if method == "query":
            method = "GET"

        elif method == "mutate":
            method = "POST"

        get_base = f"{method.capitalize()} {url_path} HTTP/1.1{NEW_LINE}"

        port = resolved_url.url.port or (
            443 if resolved_url.url.scheme == "https" else 80
        )

        hostname = resolved_url.url.parsed.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        header_items = []

        optimized_data: Optional[ResolvedArg[ResolvedData]] = self._resolved[
            arg.call_id
        ].get("data")

        if optimized_data:
            header_items.append(
                ("Content-Length", optimized_data.value.size),
            )

        header_data: Dict[str, Any] = arg.value
        header_items.extend(list(header_data.items()))

        for key, value in header_items:
            get_base += f"{key}: {value}{NEW_LINE}"

        optimized_cookies: Optional[ResolvedArg[ResolvedCookies]] = self._resolved[
            arg.call_id
        ].get("cookies")

        if optimized_cookies:
            get_base += optimized_cookies.value.cookies

        self._resolved[arg.call_id]["headers"] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(headers=(get_base + NEW_LINE).encode()),
        )

    def _parse_to_websocket_headers(self, arg: CallArg):
        resolved_url: ResolvedURL = self._resolved[arg.call_id]["url"].value
        header_data: Dict[str, str] = arg.value
        lowered_headers: Dict[str, Any] = {}

        for header_name, header_value in header_data.items():
            header_name_lowered = header_name.lower()
            lowered_headers[header_name_lowered] = header_value

        headers = [f"GET {resolved_url.url.path} HTTP/1.1", "Upgrade: websocket"]

        optimized_data: Optional[ResolvedArg[ResolvedData]] = self._resolved[
            arg.call_id
        ].get("data")

        if optimized_data:
            headers.append(f"Content-Length: {optimized_data.value.size}")

        if resolved_url.url.port == 80 or resolved_url.url.port == 443:
            hostport = pack_hostname(resolved_url.url.hostname)
        else:
            hostport = "%s:%d" % (
                pack_hostname(resolved_url.url.hostname),
                resolved_url.url.port,
            )

        host = lowered_headers.get("host")
        if host:
            headers.append(f"Host: {host}")
        else:
            headers.append(f"Host: {hostport}")

        if not lowered_headers.get("suppress_origin"):
            origin = lowered_headers.get("origin")

            if origin:
                headers.append(f"Origin: {origin}")

            elif resolved_url.url.scheme == "wss":
                headers.append(f"Origin: https://{hostport}")

            else:
                headers.append(f"Origin: http://{hostport}")

        key = create_sec_websocket_key()

        header = lowered_headers.get("header")
        if not header or "Sec-WebSocket-Key" not in header:
            headers.append(f"Sec-WebSocket-Key: {key}")
        else:
            key = header

        if not header or "Sec-WebSocket-Version" not in header:
            headers.append(f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}")

        connection = lowered_headers.get("connection")
        if not connection:
            headers.append("Connection: Upgrade")
        else:
            headers.append(connection)

        subprotocols = lowered_headers.get("subprotocols")
        if subprotocols:
            headers.append("Sec-WebSocket-Protocol: %s" % ",".join(subprotocols))

        if header:
            if isinstance(header, dict):
                header = [": ".join([k, v]) for k, v in header.items() if v is not None]
            headers.extend(header)

        headers.extend(["", ""])

        self._resolved[arg.call_id]["headers"] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(headers="\r\n".join(headers).encode()),
        )

    def _parse_to_http2_headers(self, arg: CallArg):
        resolved_url: ResolvedURL = self._resolved[arg.call_id]["url"].value
        request_type = self._request_types_map[arg.engine]

        hpack_encoder = Encoder()
        remote_settings = Settings(client=False)

        url_path = resolved_url.url.path

        optimized_params: Optional[ResolvedArg[ResolvedParams]] = self._resolved[
            arg.call_id
        ].get("params")

        if optimized_params:
            url_path += optimized_params.value.params

        request_headers: List[Tuple[str, str]] = [
            (":method", arg.method),
            (":authority", resolved_url.url.authority),
            (":scheme", resolved_url.url.scheme),
            (":path", url_path),
        ]

        headers_data: Dict[str, str] = arg.value
        if request_type == RequestType.GRPC:
            grpc_headers = {
                "Content-Type": "application/grpc",
                "Grpc-Timeout": f"{arg.timeouts.request_timeout}S",
                "TE": "trailers",
            }

            headers_data.update(grpc_headers)

        header_items = list(headers_data.items())

        request_headers.extend(
            [
                (k.lower(), v)
                for k, v in header_items
                if k.lower()
                not in (
                    "host",
                    "transfer-encoding",
                )
            ]
        )

        optimized_cookies: Optional[ResolvedArg[ResolvedCookies]] = self._resolved[
            arg.call_id
        ].get("cookies")

        if optimized_cookies:
            request_headers.append(optimized_cookies.value.cookies)

        encoded_headers: List[Tuple[bytes, bytes]] = [
            (name.encode(), value.encode()) for name, value in request_headers
        ]

        hpack_encoded_headers = hpack_encoder.encode(encoded_headers)
        hpack_encoded_headers = [
            hpack_encoded_headers[i : i + remote_settings.max_frame_size]
            for i in range(
                0, len(hpack_encoded_headers), remote_settings.max_frame_size
            )
        ]

        self._resolved[arg.call_id]["headers"] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(headers=hpack_encoded_headers[0]),
        )

    def _parse_to_http3_headers(self, arg: CallArg) -> Union[bytes, Dict[str, str]]:
        resolved_url: ResolvedURL = self._resolved[arg.call_id]["url"].value

        url_path = resolved_url.url.path

        optimized_params: Optional[ResolvedArg[ResolvedParams]] = self._resolved[
            arg.call_id
        ].get("params")

        if optimized_params:
            url_path += optimized_params.value.params

        encoded_headers: List[Tuple[str, str]] = [
            (":method", arg.method),
            (":scheme", resolved_url.url.scheme),
            (":authority", resolved_url.url.authority),
            (":path", url_path),
            ("user-agent", "hedra/client"),
        ]

        headers_data: Dict[str, str] = arg.value
        encoded_headers.extend(
            [(k.lower(), v.lower()) for (k, v) in headers_data.items()]
        )

        optimized_cookies: Optional[ResolvedArg[ResolvedCookies]] = self._resolved[
            arg.call_id
        ].get("cookies")

        if optimized_cookies:
            encoded_headers.append(optimized_cookies.value.cookies)

        self._resolved[arg.call_id]["headers"] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(
                headers=[
                    (
                        name.encode(),
                        value.encode(),
                    )
                    for name, value in encoded_headers
                ]
            ),
        )

    def _parse_to_http_cookies(self, arg: CallArg):
        cookies = []

        get_base = ""

        for cookie_data in arg.value:
            if len(cookie_data) == 1:
                cookies.append(cookie_data[0])

            elif len(cookie_data) == 2:
                cookie_name, cookie_value = cookie_data
                cookies.append(f"{cookie_name}={cookie_value}")

        cookies = "; ".join(cookies)
        get_base += f"cookie: {cookies}{NEW_LINE}"

        self._resolved[arg.call_id]["cookies"] = ResolvedArg(
            arg_type=ResolvedArgType.COOKIES,
            call_arg=arg,
            value=ResolvedCookies(
                cookies=get_base,
            ),
        )

    def _parse_to_http2_or_http3_cookies(self, arg: CallArg):
        cookies: List[str] = []

        for cookie_data in arg.value:
            if len(cookie_data) == 1:
                cookies.append(cookie_data[0])

            elif len(cookie_data) == 2:
                cookie_name, cookie_value = cookie_data
                cookies.append(f"{cookie_name}={cookie_value}")

        self._resolved[arg.call_id]["cookies"] = ResolvedArg(
            arg_type=ResolvedArgType.COOKIES,
            call_arg=arg,
            value=ResolvedCookies(
                cookies=(
                    "cookie",
                    "; ".join(cookies),
                ),
            ),
        )

    def _parse_to_http_or_udp_data(
        self,
        arg: CallArg,
    ):
        data: str | dict | Iterator | bytes | None = arg.value
        encoded_data: bytes | None = None
        is_stream = False
        size = 0

        if isinstance(data, Iterator):
            chunks = []
            for chunk in data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                size += len(encoded_chunk)
                chunks.append(encoded_chunk)

            is_stream = True
            encoded_data = chunks

        elif isinstance(data, dict):
            encoded_data = orjson.dumps(data)
            size = len(encoded_data)

        elif isinstance(data, tuple):
            encoded_data = urlencode(data).encode()
            size = len(encoded_data)

        elif isinstance(data, str):
            encoded_data = data.encode()
            size = len(encoded_data)

        self._resolved[arg.call_id]["data"] = ResolvedArg(
            arg_type=ResolvedArgType.DATA,
            call_arg=arg,
            value=ResolvedData(data=encoded_data, size=size, is_stream=is_stream),
        )

    def _parse_to_http3_data(
        self,
        arg: CallArg,
    ):
        data: str | dict | Iterator | bytes | None = arg.value
        encoded_data: bytes | None = None
        is_stream = False
        size = 0

        if isinstance(data, Iterator):
            chunks = []
            for chunk in data:
                chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                size += len(encoded_chunk)
                chunks.append(encoded_chunk)

            is_stream = True
            encoded_data = chunks

        elif isinstance(data, dict):
            encoded_data = orjson.dumps(data)
            size = len(encoded_data)

        elif isinstance(data, tuple):
            encoded_data = urlencode(data).encode()
            size = len(encoded_data)

        elif isinstance(data, str):
            encoded_data = data.encode()
            size = len(encoded_data)

        encoded_data = encode_frame(FrameType.DATA, encoded_data)

        self._resolved[arg.call_id]["data"] = ResolvedArg(
            arg_type=ResolvedArgType.DATA,
            call_arg=arg,
            value=ResolvedData(
                data=encoded_data,
                size=size,
                is_stream=is_stream,
            ),
        )

    async def _parse_to_graphql_data(self, arg: CallArg):
        source = Source(arg.value)
        document_node = parse(source)
        query_string = print_ast(document_node)

        query = {"query": query_string}

        call_kwargs = self._kwargs.get(arg.call_id)

        operation_name = next(
            arg for arg in call_kwargs if arg.arg_name == "operation_name"
        )
        variables = next(arg for arg in call_kwargs if arg.arg_name == "variables")

        if operation_name:
            query["operationName"] = operation_name

        if variables:
            query["variables"] = variables

        encoded_data = orjson.dumps(query)

        self._resolved[arg.call_id]["data"] = ResolvedArg(
            arg_type=ResolvedArgType.DATA,
            call_arg=arg,
            value=ResolvedData(
                data=encoded_data,
                size=len(encoded_data),
            ),
        )

    def _parse_to_grpc_data(self, arg: CallArg):
        data: Message | None = arg.value

        encoded_protobuf = str(
            binascii.b2a_hex(data.SerializeToString()), encoding="raw_unicode_escape"
        )

        encoded_message_length = (
            hex(int(len(encoded_protobuf) / 2)).lstrip("0x").zfill(8)
        )

        encoded_protobuf = f"00{encoded_message_length}{encoded_protobuf}"

        self._resolved[arg.call_id]["data"] = ResolvedArg(
            arg_type=ResolvedArgType.DATA,
            call_arg=arg,
            value=ResolvedData(data=binascii.a2b_hex(encoded_protobuf)),
        )
