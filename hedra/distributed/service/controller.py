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
from hedra.distributed.models.base.error import Error
from hedra.distributed.models.base.message import Message
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
from types import MethodType
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
        self._waiter: Union[asyncio.Future, None] = None

        self.engine_type = engine
        self._response_parsers: Dict[str, Message] = {}

        self.instance_ids = [
            self._instance_id + idx for idx in range(0, workers)
        ]

        if env.MERCURY_SYNC_USE_UDP_MULTICAST:
            self._udp = MercurySyncUDPMulticastConnection(
                self.host,
                self.port,
                self._instance_id,
                env=env
            ) 
        else:
            self._udp = MercurySyncUDPConnection(
                self.host,
                self.port,
                self._instance_id,
                env=env
            )
            
        if env.MERCURY_SYNC_USE_HTTP_SERVER:

            self._tcp = MercurySyncHTTPConnection(
                self.host,
                self.port + 1,
                self._instance_id,
                env=env
            ) 

        else:
            self._tcp =MercurySyncTCPConnection(
                self.host,
                self.port + 1,
                self._instance_id,
                env=env
            )

        self.setup()

    def setup(self):
        self.reserved_methods = [
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

        for _, method in inspect.getmembers(self, predicate=inspect.ismethod):
            (
                controller_models,
                controller_methods,
                middleware_enabled,
                response_parsers
            ) = self.apply_method(
                method,
                controller_models,
                controller_methods,
                middleware_enabled,
                response_parsers
            )
            

        self._parsers: Dict[str, Message] = {}
        self._events: Dict[str, Message] = {}

        for method_name, model in controller_models.items():

            self._udp.parsers[method_name] = model
            self._tcp.parsers[method_name] = model

            if isinstance(self._tcp, MercurySyncHTTPConnection):
                self._tcp._supported_handlers = supported_http_handlers
                self._tcp._middleware_enabled = middleware_enabled

            self._parsers[method_name] = model

        for method_name, method in controller_methods.items():

            self._udp.events[method_name] = method
            self._tcp.events[method_name] = method

            self._events[method_name] = method

        for key, parser in response_parsers.items():
            self._tcp._response_parsers[key] = parser

    def apply_method(
        self, 
        method: MethodType,
        controller_models: Dict[str, Message],
        controller_methods: Dict[str, Callable[
            [Message],
            Message
        ]],
        middleware_enabled: Dict[str, bool],
        response_parsers: Dict[
            str, 
            Callable[
                [Dict[str, Any]],
                BaseModel
            ]
        ]
    ) -> Tuple[
        Dict[str, Message],
        Dict[str, Callable[
            [Message],
            Message
        ]],
        Dict[str, bool],
        Dict[
            str, 
            Callable[
                [Dict[str, Any]],
                BaseModel
            ]
        ]
    ]:
        
        method_name = method.__name__

        not_internal = method_name.startswith('__') is False
        not_reserved = method_name not in self.reserved_methods
        is_server = hasattr(method, 'server_only')
        is_client = hasattr(method, 'client_only')
        is_http = hasattr(method, 'as_http') and method.as_http is True


        rpc_signature = signature(method)

        if not_internal and not_reserved and is_server:

            for param_type in rpc_signature.parameters.values():

                if issubclass(param_type.annotation, (BaseModel,)):

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
                    
                    if len(args) > 0 and issubclass(args[0], (BaseModel,)):

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

        return (
            controller_models,
            controller_methods,
            middleware_enabled,
            response_parsers
        )
    
    async def run_forever(self):
        loop = asyncio.get_event_loop()
        self._waiter = loop.create_future()

        await self._waiter

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

            for _ in range(self._workers):

                service_worker = loop.run_in_executor(
                    engine,
                    functools.partial(
                        start_pool,
                        MercurySyncUDPConnection(
                            self.host,
                            self.port,
                            self._instance_id,
                            self._env
                        ),
                        MercurySyncTCPConnection(
                            self.host,
                            self.port + 1,
                            self._instance_id,
                            self._env
                        ),
                        config=config
                    )
                )

                pool.append(service_worker) 
            
            await asyncio.gather(*pool)

        else:

            await self._udp.connect_async(
                cert_path=cert_path,
                key_path=key_path
            )

            await self._tcp.connect_async(
                cert_path=cert_path,
                key_path=key_path,
            )

    async def start_client(
        self,
        remotes: Dict[
            Tuple[str, int]: List[Type[Message]]
        ],
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None    
    ):

        for address, message_types in remotes.items():     

            host, port = address

            await self._tcp.connect_client(
                    (host, port + 1),
                    cert_path=cert_path,
                    key_path=key_path
            )

    async def send(
        self,
        event_name: str,
        message: Message
    ):
        shard_id, data = await self._udp.send(
            event_name,
            message.to_data(),
            (message.host, message.port)
        )

        if isinstance(data, Message):
            return shard_id, data

        response_data = self._response_parsers.get(event_name)(
            **data
        )

        return shard_id, response_data
    
    async def send_tcp(
        self,
        event_name: str,
        message: Message
    ):

        shard_id, data = await self._tcp.send(
            event_name,
            message.to_data(),
            (message.host, message.port + 1)
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )

        return shard_id, response_data
    
    async def stream(
        self,
        event_name: str,
        message: Message
    ) -> AsyncIterable[Tuple[int, Union[Message, Error]]]:
        
        address = (
            message.host,
            message.port
        )

        async for response in self._udp.stream(
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
        
        address = (
            message.host,
            message.port
        )

        async for response in self._tcp.stream(
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

    async def close(self) -> None:

        if self._engine:
            self._engine.shutdown(cancel_futures=True)

        await self._udp.close()
        await self._tcp.close()

        if self._waiter:
            self._waiter.set_result(None)
