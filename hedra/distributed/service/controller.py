from __future__ import annotations
import asyncio
import functools
import inspect
import multiprocessing as mp
import os
import random
import signal
import socket
import sys
from collections import defaultdict
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor
)
from inspect import signature
from hedra.distributed.middleware.base import Middleware
from hedra.distributed.connection.tcp.mercury_sync_http_connection import MercurySyncHTTPConnection
from hedra.distributed.connection.tcp.mercury_sync_tcp_connection import MercurySyncTCPConnection
from hedra.distributed.connection.udp.mercury_sync_udp_connection import MercurySyncUDPConnection
from hedra.distributed.connection.udp.mercury_sync_udp_multicast_connection import MercurySyncUDPMulticastConnection
from hedra.distributed.env import load_env, Env
from hedra.distributed.models.error import Error
from hedra.distributed.models.message import Message
from pydantic import BaseModel
from typing import (
    Optional, 
    List, 
    Literal, 
    Union, 
    Dict,
    Any,
    Type,
    get_args,
    Callable,
    AsyncIterable,
    Tuple,
    TypeVarTuple,
    Generic
)
from .plugin_group import PluginGroup
from .service import Service
from .socket import (
    bind_tcp_socket,
    bind_udp_socket
)

P = TypeVarTuple('P')


mp.allow_connection_pickling()
spawn = mp.get_context("spawn")


def handle_worker_loop_stop(
    signame, 
    loop: asyncio.AbstractEventLoop,
    waiter: Optional[asyncio.Future]
):
    if waiter:
        waiter.set_result(None)

    loop.stop()


def handle_loop_stop(
    signame, 
    executor: Union[ProcessPoolExecutor, ThreadPoolExecutor],
):
        try:
            executor.shutdown(cancel_futures=True)

        except BrokenPipeError:
            pass

        except RuntimeError:
            pass


async def run(
    udp_connecton: MercurySyncUDPConnection,
    tcp_connection: MercurySyncTCPConnection,
    config: Dict[str, Union[int, socket.socket, str]]={}
):
    loop = asyncio.get_event_loop()
    
    waiter = loop.create_future()

    for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: handle_worker_loop_stop(
                signame,
                loop,
                waiter
            )
        )  

    await udp_connecton.connect_async(
        cert_path=config.get('cert_path'),
        key_path=config.get('key_path'),
        worker_socket=config.get('udp_socket')
    )
    await tcp_connection.connect_async(
        cert_path=config.get('cert_path'),
        key_path=config.get('key_path'),
        worker_socket=config.get('tcp_socket')
    )


    await waiter


def start_pool(
    udp_connection: MercurySyncUDPConnection,
    tcp_connection: MercurySyncTCPConnection,
    config: Dict[str, Union[int, socket.socket, str]]={},
):
    import asyncio

    try:
        import uvloop
        uvloop.install()

    except ImportError:
        pass

    try:

        loop = asyncio.get_event_loop()

    except Exception:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    stdin_fileno = config.get('stdin_fileno')

    if stdin_fileno is not None:
        sys.stdin = os.fdopen(stdin_fileno)

    loop = asyncio.get_event_loop()
    
    loop.run_until_complete(
        run( 
            udp_connection,
            tcp_connection,
            config
        )
    )


class Controller(Generic[*P]):
    services: Dict[str, Type[Controller]] = {}

    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        workers: int=0,
        env: Optional[Env]=None,
        engine: Literal["process", "async"]="async",
        plugins: Dict[
            str,
            Type[Union[*P]]
        ]={},
        middleware: List[Middleware]= []
    ) -> None:
        
        if env is None:
            env = load_env(Env)
    
        self.name = self.__class__.__name__
        self._instance_id = random.randint(0, 2**16)
        self._response_parsers: Dict[str, Message] = {}
        self._host_map: Dict[
            str, 
            Dict[
                Union[MercurySyncUDPConnection, MercurySyncTCPConnection],
                Tuple[str, int]
            ]
        ] = defaultdict(dict)

        if workers < 1:
            workers = 1

        self._workers = workers

        self.host = host
        self.port = port
        self.cert_path = cert_path
        self.key_path = key_path
        self.middleware = middleware

        self._env = env
        self._engine: Union[ProcessPoolExecutor, None] = None 
        self._udp_queue: Dict[Tuple[str, int], asyncio.Queue] = defaultdict(asyncio.Queue)
        self._tcp_queue: Dict[Tuple[str, int], asyncio.Queue] = defaultdict(asyncio.Queue)
        self._cleanup_task: Union[asyncio.Task, None] = None
        self._plugin_factory = plugins
        self._waiter: Union[asyncio.Future, None] = None
        self.is_plugin = False

        self._plugins: Dict[str, PluginGroup[*P]] = {}

        self.engine_type = engine
        self._response_parsers: Dict[str, Message] = {}

        self.instance_ids = [
            self._instance_id + idx for idx in range(0, workers)
        ]

        if env.MERCURY_SYNC_USE_UDP_MULTICAST:
            self._udp_pool = [
                MercurySyncUDPMulticastConnection(
                    self.host,
                    self.port,
                    instance_id,
                    env=env
                ) for instance_id in self.instance_ids
            ]

        else:
            self._udp_pool = [
                MercurySyncUDPConnection(
                    self.host,
                    self.port,
                    instance_id,
                    env=env
                ) for instance_id in self.instance_ids
            ]

        if env.MERCURY_SYNC_USE_HTTP_SERVER:

            self._tcp_pool = [
                MercurySyncHTTPConnection(
                    self.host,
                    self.port + 1,
                    instance_id,
                    env=env
                ) for instance_id in self.instance_ids
            ]

        else:
            self._tcp_pool = [
                MercurySyncTCPConnection(
                    self.host,
                    self.port + 1,
                    instance_id,
                    env=env
                ) for instance_id in self.instance_ids
            ]

        for service_type, factory in self._plugin_factory.items():
            self._plugins[service_type] = PluginGroup([
                factory(
                    host,
                    port,
                    cert_path=cert_path,
                    key_path=key_path,
                    env=env
                ) for _ in range(self._workers)
            ])

        methods = inspect.getmembers(self, predicate=inspect.ismethod)

        reserved_methods = [
            'connect',
            'send',
            'send_tcp',
            'stream',
            'stream_tcp',
            'close'
        ]

        middleware_enabled: Dict[str, bool] = {}

        response_parsers: Dict[
            str, 
            Callable[
                [Dict[str, Any]],
                BaseModel
            ]
        ] = {}
        controller_models: Dict[str, Message] = {}
        controller_methods: Dict[str, Callable[
            [Message],
            Message
        ]] = {}

        supported_http_handlers: Dict[str, Dict[str, str]] = defaultdict(dict)

        for _, method in methods:
            method_name = method.__name__

            not_internal = method_name.startswith('__') is False
            not_reserved = method_name not in reserved_methods
            is_server = hasattr(method, 'server_only')
            is_client = hasattr(method, 'client_only')
            is_http = hasattr(method, 'as_http') and method.as_http is True
    

            rpc_signature = signature(method)

            if not_internal and not_reserved and is_server:

                for param_type in rpc_signature.parameters.values():
                    if param_type.annotation in Message.__subclasses__():

                        model = param_type.annotation
                        controller_models[method_name] = model

                    controller_methods[method_name] = method

            elif not_internal and not_reserved and is_client:

                is_stream = inspect.isasyncgenfunction(method)

                if is_stream:

                    response_type = rpc_signature.return_annotation
                    args = get_args(response_type)

                    response_call_type: Tuple[int, Message] = args[0]
                    self._response_parsers[method.target] = get_args(response_call_type)[1]

                else:

                    response_type = rpc_signature.return_annotation
                    args = get_args(response_type)
                    response_model: Tuple[int, Message] = args[1]

                    self._response_parsers[method.target] = response_model

            if not_internal and not_reserved and is_http:

                path: str = method.path

                for middleware_operator in self.middleware:
                    method = middleware_operator.wrap(method)
                    middleware_enabled[path] = True

                response_type = rpc_signature.return_annotation
                args = get_args(response_type)

                response_model: Tuple[
                    Union[BaseModel, str, None], 
                    int
                ] = args[0]

                event_http_methods: List[str] = method.methods
                path: str = method.path

                for event_http_method in event_http_methods:
                    event_key = f'{event_http_method}_{path}'

                    for param_type in rpc_signature.parameters.values():
                        args = get_args(param_type.annotation)
                        
                        if len(args) > 0 and args[0] in BaseModel.__subclasses__():

                            path: str = method.path

                            model = args[0]

                            controller_models[event_key] = model

                    controller_methods[event_key] = method

                    if isinstance(method.responses, dict):

                        responses = method.responses

                        for status, status_response_model in responses.items():
                            status_key = f'{event_http_method}_{path}_{status}'

                            if issubclass(status_response_model, BaseModel):
                                response_parsers[status_key] = lambda response: status_response_model(
                                    **response
                                ).json()

                    if isinstance(method.serializers, dict):

                        serializers = method.serializers

                        for status, serializer in serializers.items():
                            status_key = f'{event_http_method}_{path}_{status}'

                            response_parsers[status_key] = serializer

        self._parsers: Dict[str, Message] = {}
        self._events: Dict[str, Message] = {}

        for plugin_group in self._plugins.values():
            for plugin in plugin_group.each():

                plugin: Union[Controller, Service] = plugin

                self._parsers.update(plugin._parsers)
                self._response_parsers.update(plugin._response_parsers)

                self._events.update(plugin._events)

        for udp_connection, tcp_connection in zip(
            self._udp_pool,
            self._tcp_pool
        ):
            
            for method_name, model in controller_models.items():

                udp_connection.parsers[method_name] = model
                tcp_connection.parsers[method_name] = model

                if isinstance(tcp_connection, MercurySyncHTTPConnection):
                    tcp_connection._supported_handlers = supported_http_handlers
                    tcp_connection._middleware_enabled = middleware_enabled

                self._parsers[method_name] = model

            for method_name, method in controller_methods.items():

                udp_connection.events[method_name] = method
                tcp_connection.events[method_name] = method

                self._events[method_name] = method

            for key, parser in response_parsers.items():
                tcp_connection._response_parsers[key] = parser
    
    def __getitem__(self, name: str):
        return self._plugins.get(name)

    async def run_forever(self):
        loop = asyncio.get_event_loop()
        self._waiter = loop.create_future()

        await self._waiter

    
    async def use_server_socket(
        self,
        udp_worker_socket: socket.socket,
        tcp_worker_socket: socket.socket,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
        
        for udp_connection, tcp_connection in zip(
            self._udp_pool,
            self._tcp_pool
        ):
            await udp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path,
                worker_socket=udp_worker_socket
            )

            await tcp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path,
                worker_socket=tcp_worker_socket
            )

    async def use_server(
        self,
        udp_worker_transport: asyncio.DatagramTransport,
        tcp_worker_server: asyncio.Server,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
        
        for udp_connection, tcp_connection in zip(
            self._udp_pool,
            self._tcp_pool
        ):
            await udp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path,
                worker_transport=udp_worker_transport
            )

            await tcp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path,
                worker_server=tcp_worker_server
            )


    async def start_server(
      self,
      cert_path: Optional[str]=None,
      key_path: Optional[str]=None      
    ):
        
        for middleware in self.middleware:
            await middleware.__setup__()

        pool: List[asyncio.Future] = []

        loop = asyncio.get_event_loop()

        if self.engine_type == "process":
            engine = ProcessPoolExecutor(
                max_workers=self._workers,
                mp_context=mp.get_context(method='spawn')
            )

        if self.engine_type == 'process':

            udp_socket = bind_udp_socket(self.host, self.port)
            tcp_socket = bind_tcp_socket(self.host, self.port + 1)

            stdin_fileno: Optional[int]
            try:
                stdin_fileno = sys.stdin.fileno()
            except OSError:
                stdin_fileno = None

            config = {
                "udp_socket": udp_socket,
                "tcp_socket": tcp_socket,
                "stdin_fileno": stdin_fileno,
                "cert_path": cert_path,
                "key_path": key_path
            }


            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        engine
                    )
                )  

            for udp_connection, tcp_connection in zip(
                self._udp_pool,
                self._tcp_pool
            ):

                service_worker = loop.run_in_executor(
                    engine,
                    functools.partial(
                        start_pool,
                        udp_connection,
                        tcp_connection,
                        config=config
                    )
                )

                pool.append(service_worker) 

        else:

            offset = 0

            for udp_connection, tcp_connection in zip(
                self._udp_pool,
                self._tcp_pool
            ):
                udp_connection.port += offset
                tcp_connection.port += offset

                pool.append(
                    asyncio.create_task(
                        udp_connection.connect_async(
                            cert_path=cert_path,
                            key_path=key_path
                        )
                    )
                )

                pool.append(
                    asyncio.create_task(
                        tcp_connection.connect_async(
                            cert_path=cert_path,
                            key_path=key_path
                        )
                    )
                )

                offset += 2

        await asyncio.gather(*pool)

        connections = list(zip(
            self._udp_pool,
            self._tcp_pool
        ))

        for idx in range(self._workers):

            udp_connection, tcp_connection = connections[idx]

            transport = udp_connection._transport
            server = tcp_connection._server
            
            for plugin_name in self._plugins:

                plugin: Union[
                    Service,
                    Controller
                ] = self._plugins[plugin_name].at(idx)

                if isinstance(plugin, Controller):

                    await plugin.use_server(
                        transport,
                        server,
                        cert_path=cert_path,
                        key_path=key_path
                    )

        await self._copy_to_plugins()


        services_count = len(self.services)

        starting_port = self.port + (self._workers * 2)
        max_port = starting_port + ((self._workers * 2) * services_count)

        ports_range = list(range(
            starting_port,
            max_port,
            self._workers * 2
        ))

        for service_type, port in zip(
            self.services.values(),
            ports_range
        ):
            
            service = service_type(
                self.host,
                port,
                cert_path=cert_path,
                key_path=key_path,
                workers=self._workers,
                env=self._env
            )

            await service.start_server(
                cert_path=cert_path,
                key_path=key_path
            )

    async def start_client(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ):

        remote_pool: List[Message] = []
        pool_range = self._workers * 2

        for idx in range(0, pool_range, 2):
            remote_copy = Message(**{
                'host': remote.host,
                'port': remote.port + idx
            })

            remote_pool.append(remote_copy)
        
        for udp_connection, tcp_connection, remote_copy in zip(
            self._udp_pool,
            self._tcp_pool,
            remote_pool
        ):
      
            await udp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

                        
            await tcp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await self._udp_queue[(remote.host, remote.port)].put(udp_connection)
            await self._tcp_queue[(remote.host, remote.port)].put(tcp_connection)

            self._host_map[remote.__class__.__name__][udp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

            self._host_map[remote.__class__.__name__][tcp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

        for tcp_connection, remote_copy in zip(
            self._tcp_pool,
            remote_pool
        ):
            await tcp_connection.connect_client(
                    (remote_copy.host, remote_copy.port + 1),
                    cert_path=cert_path,
                    key_path=key_path
                )

        for idx in range(self._workers):
            for plugin_name in self._plugins:

                remote = remote_pool[idx]
                plugin: Service = self._plugins[plugin_name].at(idx)
                
                await plugin.connect(
                    (remote_copy.host, remote_copy.port + 1),
                    cert_path=cert_path,
                    key_path=key_path
                )

        await self._copy_to_plugins()

    async def _copy_to_plugins(self):

        for plugin_group in self._plugins.values():
            for plugin in plugin_group.each(): 
                
                if isinstance(plugin, Controller):
                    for tcp_connection, udp_connection in zip(
                        plugin._udp_pool,
                        plugin._tcp_pool
                    ):
                        udp_connection.events.update(self._events)
                        tcp_connection.events.update(self._events)

                        udp_connection.parsers.update(self._parsers)
                        tcp_connection.parsers.update(self._parsers)

                elif isinstance(plugin, Service):
                    plugin._udp_connection.events.update(self._events)
                    plugin._tcp_connection.events.update(self._events)

                    plugin._udp_connection.parsers.update(self._parsers)
                    plugin._tcp_connection.parsers.update(self._parsers)
                
                plugin._events.update(self._events)
                plugin._parsers.update(self._parsers)

    async def extend_client(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ) -> int:

        remote_pool: List[Message] = []
        pool_range = self._workers * 2

        for idx in range(0, pool_range, 2):
            remote_copy = Message(**{
                'host': remote.host,
                'port': remote.port + idx
            })

            remote_pool.append(remote_copy)

        port = max(self.port, remote.port) + pool_range

        udp_pool = [
            MercurySyncUDPConnection(
                self.host,
                port + idx,
                instance_id,
                env=self._env
            ) for instance_id, idx in zip(
                self.instance_ids,
                range(0, pool_range, 2)
            )
        ]

        tcp_pool = [
            MercurySyncTCPConnection(
                self.host,
                port + idx + 1,
                instance_id,
                env=self._env
            ) for instance_id, idx in zip(
                self.instance_ids,
                range(0, pool_range, 2)
            )
        ]

        for tcp_connection, udp_connection in zip(
            udp_pool,
            tcp_pool
        ):
            
            tcp_connection.parsers.update(self._parsers)
            udp_connection.parsers.update(self._parsers)

            tcp_connection.events.update(self._events)
            udp_connection.events.update(self._events)
            

        for udp_connection, tcp_connection, remote_copy in zip(
            udp_pool,
            tcp_pool,
            remote_pool
        ):
      
            await udp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await tcp_connection.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await self._udp_queue[(remote.host, remote.port)].put(udp_connection)
            await self._tcp_queue[(remote.host, remote.port)].put(tcp_connection)

            self._host_map[remote.__class__.__name__][udp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

            self._host_map[remote.__class__.__name__][tcp_connection] = (
                remote_copy.host, 
                remote_copy.port
            )

        for tcp_connection, remote_copy in zip(
            tcp_pool,
            remote_pool
        ):
            await tcp_connection.connect_client(
                (remote_copy.host, remote_copy.port + 1),
                cert_path=cert_path,
                key_path=key_path
            )
            
        self._udp_pool.extend(udp_pool)
        self._tcp_pool.extend(tcp_pool)

        self._copy_to_plugins()

        return port
    
    async def refresh_clients(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ) -> int:

        existing_udp_connections = self._udp_queue[(remote.host, remote.port)]
        existing_tcp_connections = self._tcp_queue[(remote.host, remote.port)]

        while existing_udp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_udp_connections.get()
            await connection.close()

        while existing_tcp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_tcp_connections.get()
            await connection.close()

        return await self.extend_client(
            remote,
            cert_path=cert_path,
            key_path=key_path
        )
    
    async def remove_clients(
        self,
        remote: Message 
    ):
        
        existing_udp_connections = self._udp_queue[(remote.host, remote.port)]
        existing_tcp_connections = self._tcp_queue[(remote.host, remote.port)]

        while existing_udp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_udp_connections.get()
            await connection.close()

        while existing_tcp_connections.empty() is False:
            connection: MercurySyncUDPConnection = await existing_tcp_connections.get()
            await connection.close()

        if self._udp_queue.get((remote.host, remote.port)):
            del self._udp_queue[(remote.host, remote.port)]

        
        if self._tcp_queue.get((remote.host, remote.port)):
            del self._tcp_queue[(remote.host, remote.port)]

    async def send(
        self,
        event_name: str,
        message: Message
    ):
        connection: MercurySyncUDPConnection = await self._udp_queue[(message.host, message.port)].get()
        (host, port) = self._host_map.get(message.__class__.__name__).get(connection)

        address = (
            host,
            port
        )

        shard_id, data = await connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )

        self._udp_queue[(message.host, message.port)].put_nowait(connection)

        return shard_id, response_data
    
    async def send_tcp(
        self,
        event_name: str,
        message: Message
    ):
        connection: MercurySyncTCPConnection = await self._tcp_queue[(message.host, message.port)].get()
        (host, port) = self._host_map.get(message.__class__.__name__).get(connection)
        address = (
            host,
            port + 1
        )

        shard_id, data = await connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )

        await self._tcp_queue[(message.host, message.port)].put(connection)

        return shard_id, response_data
    
    async def stream(
        self,
        event_name: str,
        message: Message
    ) -> AsyncIterable[Tuple[int, Union[Message, Error]]]:
        
        async for connection in self._iter_udp_connections():
            (host, port) = self._host_map.get(message.__class__.__name__).get(connection)

            address = (host, port)

            async for response in connection.stream(
                event_name,
                message.to_data(),
                address
            ):
                shard_id, data = response
                response_data = self._response_parsers.get(event_name)(
                    **data
                )

                yield shard_id, response_data

    async def stream_tcp(
        self,
        event_name: str,
        message: Message
    ) -> AsyncIterable[Tuple[int, Union[Message, Error]]]:
        
        
        async for connection in self._iter_tcp_connections():
            (host, port) = self._host_map.get(message.__class__.__name__).get(connection)

            address = (host, port + 1)

            async for response in connection.stream(
                event_name,
                message.to_data(),
                address
            ):
                shard_id, data = response

                if data.get('error'):
                    yield shard_id, Error(**data)

                response_data = self._response_parsers.get(event_name)(
                    **data
                )

                yield shard_id, response_data
    
    async def _iter_tcp_connections(self) -> AsyncIterable[MercurySyncUDPConnection]:
        for connection in enumerate(self._tcp_pool):
            yield connection

    async def _iter_udp_connections(self) -> AsyncIterable[MercurySyncTCPConnection]:
        for connection in enumerate(self._udp_pool):
            yield connection

    async def close(self) -> None:

        if self._engine:
            self._engine.shutdown(cancel_futures=True)

        await asyncio.gather(*[
            asyncio.create_task(
                udp_connection.close()
            ) for udp_connection in self._udp_pool
        ])

        await asyncio.gather(*[
            asyncio.create_task(
                tcp_connection.close()
            ) for tcp_connection in self._tcp_pool
        ])

        for group in self._plugins.values():

            services: List[Service] = [
                service for service in group.each()
            ]

            await asyncio.gather(*[
                service.close() for service in services
            ])

        if self._waiter:
            self._waiter.set_result(None)
