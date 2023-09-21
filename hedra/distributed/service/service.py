from __future__ import annotations
import asyncio
import random
import socket
import inspect
from inspect import signature
from hedra.distributed.connection.tcp.mercury_sync_tcp_connection import MercurySyncTCPConnection
from hedra.distributed.connection.udp.mercury_sync_udp_connection import MercurySyncUDPConnection
from hedra.distributed.env import load_env, Env
from hedra.distributed.models.base.error import Error
from hedra.distributed.models.base.message import Message
from typing import (
    Tuple, 
    Dict, 
    List,
    Optional,
    get_args,
    Union,
    AsyncIterable
)

class Service:
    
    def __init__(
        self,
        host: str,
        port: int,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        env: Optional[Env]=None
    ) -> None:
        self.name = self.__class__.__name__
        self._instance_id = random.randint(0, 2**16)
        self._response_parsers: Dict[str, Message] = {}

        self.host = host
        self.port = port
        self.cert_path = cert_path
        self.key_path = key_path

        if env is None:
            env = load_env(Env)

        self._env = env

        self._udp_connection = MercurySyncUDPConnection(
            host,
            port,
            self._instance_id,
            env
        )

        self._tcp_connection = MercurySyncTCPConnection(
            host,
            port + 1,
            self._instance_id,
            env
        )

        self._host_map: Dict[str, Tuple[str, int]] = {}    

        methods = inspect.getmembers(self, predicate=inspect.ismethod)

        reserved_methods = [
            'start',
            'connect',
            'send',
            'send_tcp',
            'stream',
            'stream_tcp',
            'close'
        ]

        for _, method in methods:
            method_name = method.__name__

            not_internal = method_name.startswith('__') is False
            not_reserved = method_name not in reserved_methods
            is_server = hasattr(method, 'server_only')
            is_client = hasattr(method, 'client_only')

            rpc_signature = signature(method)

            if not_internal and not_reserved and is_server:
                
                for param_type in rpc_signature.parameters.values():
                    if param_type.annotation in Message.__subclasses__():

                        model = param_type.annotation

                        self._tcp_connection.parsers[method_name] = model
                        self._udp_connection.parsers[method_name] = model
  
                self._tcp_connection.events[method_name] = method
                self._udp_connection.events[method_name] = method

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

        self._loop: Union[ asyncio.AbstractEventLoop, None] = None

    def update_parsers(
        self,
        parsers: Dict[str, Message]
    ):
        self._udp_connection.parsers.update(parsers)
        self._tcp_connection.parsers.update(parsers)


    def start(
        self,
        tcp_worker_socket: Optional[socket.socket]=None,
        udp_worker_socket: Optional[socket.socket]=None
    ) -> None:
        
        self._loop = asyncio.get_event_loop()

        self._tcp_connection.connect(
            cert_path=self.cert_path,
            key_path=self.key_path,
            worker_socket=tcp_worker_socket
        )
        self._udp_connection.connect(
            cert_path=self.cert_path,
            key_path=self.key_path,
            worker_socket=udp_worker_socket
        )
        

    def create_pool(self, size: int) -> List[Service]:

        port_pool_size = size * 2

        ports = [
            self.port + idx for idx in range(0, port_pool_size, 2)
        ]

        return [
            self._copy(
                port=port
            ) for port in ports
        ]

    def _copy(
        self,
        host: str=None,
        port: int= None
    ):
        
        if host is None:
            host = self.host

        if port is None:
            port = self.port

        return type(self)(
            host,
            port
        )
    
    async def use_server_socket(
        self,
        udp_worker_socket: socket.socket,
        tcp_worker_socket: socket.socket,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
        await self._udp_connection.connect_async(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=udp_worker_socket
        )

        await self._tcp_connection.connect_async(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=tcp_worker_socket
        )


    async def connect(
        self,
        remote: Message,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> None:
        address = (remote.host, remote.port)
        self._host_map[remote.__class__.__name__] = address

        if cert_path is None:
            cert_path = self.cert_path

        if key_path is None:
            key_path = self.key_path

        await self._tcp_connection.connect_client(
            (remote.host, remote.port + 1),
            cert_path=cert_path,
            key_path=key_path
        )

    async def send(
        self, 
        event_name: str,
        message: Message
    ) -> Tuple[int, Union[Message, Error]]:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port
        )

        shard_id, data = await self._udp_connection.send(
            event_name,
            message.to_data(),
            address
        )

        response_data = self._response_parsers.get(event_name)(
            **data
        )
        return shard_id, response_data
    
    async def send_tcp(
        self,
        event_name: str,
        message: Message
    ) -> Tuple[int, Union[Message, Error]]:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port + 1
        )

        shard_id, data = await self._tcp_connection.send(
            event_name,
            message.to_data(),
            address
        )


        if data.get('error'):
            return shard_id,  Error(**data)

        response_data = self._response_parsers.get(event_name)(
            **data
        )
        return shard_id, response_data
    
    async def stream(
        self,
        event_name: str,
        message: Message
    ) -> AsyncIterable[Tuple[int, Union[Message, Error]]]:
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port
        )

        async for response in self._udp_connection.stream(
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
        (host, port)  = self._host_map.get(message.__class__.__name__)
        address = (
            host,
            port + 1
        )

        async for response in self._tcp_connection.stream(
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
        await self._tcp_connection.close()
        await self._udp_connection.close()
