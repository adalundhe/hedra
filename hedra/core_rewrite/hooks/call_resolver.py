import asyncio
import binascii
import json
from collections import defaultdict
from hedra.core_rewrite.engines.client.client_types.common.protocols.tcp import TCPConnection
from hedra.core_rewrite.engines.client.client_types.common.protocols.udp import UDPConnection
from hedra.core_rewrite.engines.client.client_types.common.url import URL
from hedra.core_rewrite.engines.client.client_types.common.types import (
    RequestType,
    RequestTypesMap,
    ProtocolMap
)
from hedra.core_rewrite.engines.client.client_types.common.constants import NEW_LINE
from hedra.core_rewrite.engines.client.client_types.common.encoder import Encoder
from hedra.core.engines.types.common.ssl import (
    get_http2_ssl_context,
    get_default_ssl_context,
    get_graphql_ssl_context
)
from hedra.core_rewrite.engines.client.client_types.http2.streams.stream_settings import Settings
from hedra.core_rewrite.engines.client.client_types.websocket.constants import WEBSOCKETS_VERSION
from hedra.core_rewrite.engines.client.client_types.websocket.utils import (
    pack_hostname,
    create_sec_websocket_key
)
from ssl import SSLContext
from typing import (
    List, 
    Dict, 
    Any,
    Callable, 
    Tuple,
    Coroutine,
    Union,
    Iterator
)
from urllib.parse import urlencode
from .call_arg import CallArg
from .resolved_arg import ResolvedArg
from .resolved_arg_type import ResolvedArgType
from .resolved_auth import ResolvedAuth
from .resolved_data import ResolvedData
from .resolved_headers import ResolvedHeaders
from .resolved_method import ResolvedMethod
from .resolved_params import ResolvedParams
from .resolved_query import ResolvedQuery
from .resolved_url import ResolvedURL

try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source=None
    parse=lambda: None
    print_ast=lambda: None


class CallResolver:

    def __init__(self) -> None:
        self._args: Dict[str, List[CallArg]] = defaultdict(list)
        self._kwargs: Dict[str, List[CallArg]] = defaultdict(list)

        self._position_map: Dict[str, Dict[int, str]] = {
            'graphql': { 0: 'url', 1: 'query' },
            'graphqlh2': { 0: 'url', 1: 'query' },
            'grpc': { 0: 'url' },
            'http': { 0: 'url' },
            'http2': { 0: 'url' },
            'http3': { 0: 'url' },
            'udp': { 0: 'url' },
            'websocket': { 0: 'url' }
        }

        self._call_optimization_map: Dict[
            str,
            Callable[
                [CallArg],
                Coroutine[Any, Any, None]
            ]
        ] = {
            'url': self._set_url,
            'query': self._set_data,
            'auth': self._set_auth,
            'params': self._set_params,
            'headers': self._set_headers,
            'data': self._set_data
        }

        self._request_types_map = RequestTypesMap()
        self._protocol_map = ProtocolMap()
        self._resolved: Dict[str, Dict[str, ResolvedArg]] = defaultdict(dict)

        self._connections: Dict[
            RequestType,
            Callable[
                [],
                Union[
                    TCPConnection,
                    UDPConnection
                ]
            ]
        ] = {
            RequestType.GRAPHQL: lambda: TCPConnection(factory_type=RequestType.GRAPHQL),
            RequestType.GRAPHQL_HTTP2: lambda: TCPConnection(factory_type=RequestType.GRAPHQL_HTTP2),
            RequestType.GRPC: lambda: TCPConnection(factory_type=RequestType.GRPC),
            RequestType.HTTP: lambda: TCPConnection(factory_type=RequestType.HTTP),
            RequestType.HTTP2: lambda: TCPConnection(factory_type=RequestType.HTTP2),
            RequestType.HTTP3: lambda: TCPConnection(factory_type=RequestType.HTTP3),
            RequestType.UDP: lambda: UDPConnection(factory_type=RequestType.UDP),
            RequestType.WEBSOCKET: lambda: TCPConnection(factory_type=RequestType.WEBSOCKET)
        }

        self._ssl_context: Dict[
            RequestType: Union[
                TCPConnection,
                UDPConnection
            ]
        ] = {
            RequestType.GRAPHQL: lambda: get_graphql_ssl_context(),
            RequestType.GRAPHQL_HTTP2: lambda: get_graphql_ssl_context(),
            RequestType.GRPC: lambda: get_http2_ssl_context(),
            RequestType.HTTP: lambda: get_default_ssl_context(),
            RequestType.HTTP2: lambda: get_http2_ssl_context(),
            RequestType.HTTP3: lambda: get_default_ssl_context(),
            RequestType.UDP: lambda: get_default_ssl_context(),
            RequestType.WEBSOCKET: lambda: get_default_ssl_context()
        }

        self._call_engine_types: Dict[str, str] = {}
        self._call_workflows: Dict[str, str] = {}

    def __iter__(self):
        for call_id, args in self._resolved.items():
            yield call_id, args

    def __getitem__(
        self,
        call_id: str
    ):
        return self._resolved.get(call_id)
    
    def get_engine(
        self,
        call_id: str
    ):
        return self._call_engine_types.get(call_id)
    
    def get_workflow(
        self,
        call_id: str
    ):
        return self._call_workflows.get(call_id)

    def add_args(
        self,
        optimizer_args: Dict[str, List[CallArg]]
    ):
        
        for call_id in optimizer_args:

            self._args[call_id].extend([
                arg for arg in optimizer_args[call_id] if arg.arg_type == 'arg'
            ])

            self._kwargs[call_id].extend([
                arg for arg in optimizer_args[call_id] if arg.arg_type == 'kwarg'
            ])

    async def resolve_arg_types(self):
        await asyncio.gather(*[
            self._optimize_engine_type(
                engine_type
            ) for engine_type in self._position_map
        ])

    async def _optimize_engine_type(
        self,
        engine_type: str
    ):
        
        args: Dict[
            str,
            List[Tuple[CallArg, Callable[..., None]]] 
        ] = defaultdict(list)

        engine_type_args: List[Tuple[CallArg, Callable[..., None]]]  = []
        engine_arg_positions = self._position_map.get(engine_type)


        for call_id in self._args:

            for arg in self._args[call_id]:
                self._call_engine_types[call_id] = self._request_types_map[arg.engine]
                self._call_workflows[call_id] = arg.workflow
                self._resolved[call_id]['method'] = ResolvedArg(
                    ResolvedArgType.METHOD,
                    arg,
                    ResolvedMethod(
                        method=arg.method
                    )
                )

        for call_id in self._args:
            engine_type_args.extend([
                (
                    arg,
                    self._call_optimization_map.get(
                        engine_arg_positions.get(arg.position)
                    )
                ) for arg in self._args[call_id] if arg.engine == engine_type 
            ])
        
        for call_id in self._kwargs:
            engine_type_args.extend([
                (
                    arg,
                    self._call_optimization_map.get(arg.arg_name)
                ) for arg in self._kwargs[call_id] if arg.engine == engine_type 
            ])

        for arg, optimizer in engine_type_args:
            arg_type = arg.arg_name
            if arg_type is None:
                arg_type = engine_arg_positions.get(arg.position)

            args[arg_type].append((
                arg,
                optimizer
            ))

        await asyncio.gather(*[
            call_optimizer(arg) for arg, call_optimizer in args['url'] 
        ])

        for arg, call_optimizer in args['data']:
            call_optimizer(arg)

        data_args: List[Tuple[Callable, Callable[..., None]]] = []
        for arg_type, arg_types in args.items():
            if arg_type not in ['url', 'data']:
                data_args.extend(arg_types)

        for arg, call_optimizer in data_args:
            call_optimizer(arg) 


    async def _set_url(
        self,
        arg: CallArg
    ):
        request_type = self._request_types_map[arg.engine]
        address_family, protocol = self._protocol_map[request_type]

        url = URL(
            arg.value,
            family=address_family.value,
            protocol=protocol.value
        )

        connection = self._connections.get(request_type)()

        ssl_context: Union[SSLContext, None] = None
        hosts: Dict[str, str] = {}

        if url.is_ssl:
            ssl_context = self._ssl_context.get(request_type)()

        try:
                
            if hosts.get(url.hostname) is None:
                socket_configs = await asyncio.wait_for(url.lookup(), timeout=arg.timeouts.connect_timeout)
            
                for ip_addr, configs in socket_configs.items():
                    for config in configs:
                        
                        try:

                            match request_type:

                                case RequestType.GRAPHQL | RequestType.HTTP | RequestType.HTTP3 | RequestType.WEBSOCKET:
                                    await connection.create(
                                        hostname=url.hostname,
                                        socket_config=config,
                                        ssl=ssl_context
                                    )

                                case RequestType.GRAPHQL_HTTP2 | RequestType.GRPC | RequestType.HTTP2:
                                    await connection.create_http2(
                                        hostname=url.hostname,
                                        socket_config=config,
                                        ssl=ssl_context
                                    )

                                case RequestType.PLAYWRIGHT:
                                    pass

                                case RequestType.UDP:
                                    await connection.create_udp(
                                        socket_config=config,
                                        tls=ssl_context
                                    )

                                case _:
                                    raise Exception(f'Err. - invalid request type - {request_type.value}')

                            url.socket_config = config
                            url.ip_addr = ip_addr
                            url.has_ip_addr = True
                            break

                        except Exception as e:
                            pass

                    if url.socket_config:
                        break

                if url.socket_config is None:
                    raise Exception('Err. - No socket found.')

                hosts[url.hostname] = {
                    'ip_addr': url.ip_addr,
                    'socket_config': url.socket_config
                }

            else:
                host_config: Dict[str, str | Any] = hosts.get(url.hostname, {})
                url.ip_addr = host_config.get('ip_addr')
                url.socket_config = host_config.get('socket_config')

        except Exception as e:       
            raise e

        self._resolved[arg.call_id]['url'] = ResolvedArg(
            arg_type=ResolvedArgType.URL,
            call_arg=arg,
            value=ResolvedURL(
                ssl_context=ssl_context,
                url=url
            )
        )

    def _set_headers(
        self,
        arg: CallArg
    ):
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
                raise Exception('Err. - Invalid request type.')
            
    def _set_auth(
        self,
        arg: CallArg
    ):
        auth_params: Tuple[str] | Tuple[str, str] = arg.value
        params_count = len(auth_params)

        if params_count == 1:
            username = auth_params[0]
            self._resolved[arg.call_id]['auth'] = ResolvedArg(
                arg_type=ResolvedArgType.AUTH,
                call_arg=arg,
                value=ResolvedAuth(
                    username=username,
                    auth=f'{username}:'.encode()
                )
            ) 

        elif params_count == 2:
            username, password = auth_params
            self._resolved[arg.call_id]['auth'] = ResolvedArg(
                arg_type=ResolvedArgType.AUTH,
                call_arg=arg,
                value=ResolvedAuth(
                    username=username,
                    password=password,
                    auth=f'{username}:{password}'.encode()
                )
            ) 

        else:
            raise Exception('Err. - can only except username tuple of length one or username/password tuple of length two.')
            
    def _set_params(
        self,
        arg: CallArg
    ):
        params_dict: Dict[str, Any] = arg.value
        params = urlencode(params_dict)

        self._resolved[arg.call_id]['params'] = ResolvedArg(
            arg_type=ResolvedArgType.PARAMS,
            call_arg=arg,
            value=ResolvedParams(
                params=f'?{params}'.encode()
            )
        )

    def _set_data(
        self,
        arg: CallArg
    ):
        request_type = self._request_types_map[arg.engine]

        match request_type:

            case RequestType.GRAPHQL | RequestType.GRAPHQL_HTTP2:
                self._parse_to_graphql_data(arg)

            case RequestType.GRPC:
                self._parse_to_grpc_data(arg)

            case RequestType.HTTP | RequestType.HTTP2 | RequestType.HTTP3 | RequestType.UDP | RequestType.WEBSOCKET:
                self._parse_to_http_or_udp_data(arg)

            case RequestType.PLAYWRIGHT | RequestType.UDP:
                pass

            case _:
                raise Exception('Err. - Invalid request type.')
    
    
    def _parse_to_http_headers(
        self,
        arg: CallArg
    ):
        resolved_url: ResolvedURL = self._resolved[arg.call_id]['url'].value
        resolved_data: ResolvedData = self._resolved[arg.call_id]['data'].value

        get_base = f"{arg.method} {resolved_url.url.path} HTTP/1.1{NEW_LINE}"

        port = resolved_url.url.port or (443 if resolved_url.url.scheme == "https" else 80)
        hostname = resolved_url.url.parsed.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f'{hostname}:{port}'

        header_items = [
            ("HOST", hostname),
            ("User-Agent", "mercury-http"),
            ("Keep-Alive", "timeout=60, max=100000"),
            ("Content-Length", resolved_data.size)
        ]

        header_data: Dict[str, Any] = arg.value
        header_items.extend(
            list(header_data.items())
        )

        for key, value in header_items:
            get_base += f"{key}: {value}{NEW_LINE}"

        self._resolved[arg.call_id]['headers'] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(
                headers=(
                    get_base + NEW_LINE
                ).encode()
            )
        )
    
    def _parse_to_websocket_headers(
        self,
        arg: CallArg
    ):
        resolved_url: ResolvedURL = self._resolved[arg.call_id]['url'].value
        header_data: Dict[str, str] = arg.value
        lowered_headers: Dict[str, Any] = {}

        for header_name, header_value in header_data.items():
            header_name_lowered = header_name.lower()
            lowered_headers[header_name_lowered] = header_value

        headers = [
            f"GET {resolved_url.url.path} HTTP/1.1",
            "Upgrade: websocket"
        ]
        if resolved_url.url.port == 80 or resolved_url.url.port == 443:
            hostport = pack_hostname(resolved_url.url.hostname)
        else:
            hostport = "%s:%d" % (pack_hostname(
                resolved_url.url.hostname
            ), resolved_url.url.port)

        host = lowered_headers.get("host")
        if host:
            headers.append(f"Host: {host}")
        else:
            headers.append(f"Host: {hostport}")

        if  not lowered_headers.get("suppress_origin"):

            origin = lowered_headers.get("origin")

            if origin:
                headers.append(f"Origin: {origin}")

            elif resolved_url.url.scheme == "wss":
                headers.append(f"Origin: https://{hostport}")
                
            else:
                headers.append(f"Origin: http://{hostport}")

        key = create_sec_websocket_key()

        header = lowered_headers.get("header")
        if not header or 'Sec-WebSocket-Key' not in header:
            headers.append(f"Sec-WebSocket-Key: {key}")
        else:
            key = header #.get('Sec-WebSocket-Key')

        if not header or 'Sec-WebSocket-Version' not in header:
            headers.append(f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}")

        connection = lowered_headers.get('connection')
        if not connection:
            headers.append('Connection: Upgrade')
        else:
            headers.append(connection)

        subprotocols = lowered_headers.get("subprotocols")
        if subprotocols:
            headers.append("Sec-WebSocket-Protocol: %s" % ",".join(subprotocols))

        if header:
            if isinstance(header, dict):
                header = [
                    ": ".join([k, v])
                    for k, v in header.items()
                    if v is not None
                ]
            headers.extend(header)

        headers.extend([
            "",
            ""
        ])

        self._resolved[arg.call_id]['headers'] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(
                headers='\r\n'.join(
                    headers
                ).encode()
            )
        )

    def _parse_to_http2_headers(
        self,
        arg: CallArg
    ):
        resolved_url: ResolvedURL = self._resolved[arg.call_id]['url'].value
        request_type = self._request_types_map[arg.engine]

        hpack_encoder = Encoder()
        remote_settings = Settings(client=False)

        encoded_headers = [
            (b":method", arg.method),
            (b":authority", resolved_url.url.authority),
            (b":scheme", resolved_url.url.scheme),
            (b":path", resolved_url.url.path),
        ]

        headers_data: Dict[str, str] = arg.value
        if request_type == RequestType.GRPC:
            grpc_headers = {
                'Content-Type': 'application/grpc',
                'Grpc-Timeout': f'{arg.timeouts.request_timeout}S',
                'TE': 'trailers'
            }

            headers_data.update(grpc_headers)

        header_items = list(headers_data.items())

        encoded_headers.extend([
            (
                k.lower(), 
                v
            )
            for k, v in header_items
            if k.lower()
            not in (
                b"host",
                b"transfer-encoding",
            )
        ])
        
        encoded_headers = hpack_encoder.encode(encoded_headers)
        encoded_headers = [
            encoded_headers[i:i+remote_settings.max_frame_size]
            for i in range(
                0, len(encoded_headers), remote_settings.max_frame_size
            )
        ]

        self._resolved[arg.call_id]['headers'] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(
                headers=encoded_headers
            )
        )

    def _parse_to_http3_headers(
        self,
        arg: CallArg
    ) -> Union[bytes, Dict[str, str]]:

        resolved_url: ResolvedURL = self._resolved[arg.call_id]['url'].value

        encoded_headers = [
            (b":method", arg.method.encode()),
            (b":scheme", resolved_url.url.scheme.encode()),
            (b":authority", resolved_url.url.authority.encode()),
            (b":path", resolved_url.url.full.encode()),
            (b"user-agent", 'hedra/client'.encode()),
        ]
        
        headers_data: Dict[str, str] = arg.value
        encoded_headers.extend([
            (
                k.encode(), 
                v.encode()
            ) for (
                k, 
                v
            ) in headers_data.items()
        ])

        self._resolved[arg.call_id]['headers'] = ResolvedArg(
            arg_type=ResolvedArgType.HEADERS,
            call_arg=arg,
            value=ResolvedHeaders(
                headers=encoded_headers
            )
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
            encoded_data = json.dumps(
                data
            ).encode()

        elif isinstance(data, tuple):
            encoded_data = urlencode(
                data
            ).encode()

        elif isinstance(data, str):
            encoded_data = data.encode()

        self._resolved[arg.call_id]['data'] = ResolvedArg(
            arg_type=ResolvedArgType.DATA,
            call_arg=arg,
            value=ResolvedData(
                data=encoded_data,
                size=size,
                is_stream=is_stream
            )
        )

    
    async def _parse_to_graphql_data(
        self,
        arg: CallArg
    ):
        source = Source(arg.value)
        document_node = parse(source)
        query_string = print_ast(document_node)
        
        query = {
            "query": query_string
        }

        call_kwargs = self._kwargs.get(arg.call_id)

        operation_name = next(arg for arg in call_kwargs if arg.arg_name == 'operation_name')
        variables = next(arg for arg in call_kwargs if arg.arg_name == 'variables')
        
        if operation_name:
            query["operationName"] = operation_name
        
        if variables:
            query["variables"] = variables

        self._resolved[arg.call_id]['data'] = ResolvedArg(
            arg_type=ResolvedArgType.QUERY,
            call_arg=arg,
            value=ResolvedQuery(
                query=json.dumps(
                    query
                ).encode(),
                size=len(query_string)
                
            )
        )

    def _parse_to_grpc_data(
        self,
        arg: CallArg
    ):
        
        data: Any | None = arg.value

        encoded_protobuf = str(binascii.b2a_hex(
            data.SerializeToString()), 
            encoding='raw_unicode_escape'
        )

        encoded_message_length = hex(int(
            len(encoded_protobuf)/2
        )).lstrip(
            "0x"
        ).zfill(8)

        encoded_protobuf = f'00{encoded_message_length}{encoded_protobuf}'

        self._resolved[arg.call_id]['data'] = ResolvedArg(
            arg_type=ResolvedArgType.DATA,
            call_arg=arg,
            value=ResolvedData(
                data=binascii.a2b_hex(
                    encoded_protobuf
                )
            )
        )