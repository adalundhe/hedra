import asyncio
import socket
from typing import Callable, Optional

from hedra.core.engines.types.common.types import RequestTypes

from .quic_protocol import QuicProtocol

try:
    from aioquic.h3.connection import H3_ALPN
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.connection import QuicConnection

except ImportError:
    H3_ALPN = []
    QuicConnection = object
    QuicConfiguration = object



QuicStreamHandler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], None]


class UDPConnection:

    def __init__(self, factory_type: RequestTypes = RequestTypes.HTTP) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport: asyncio.DatagramTransport = None
        self.factory_type = factory_type
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None
    
    async def create_http3(
        self, 
        socket_config=None,
        server_name: str=None, 
        configuration: Optional[QuicConfiguration] = None,
        stream_handler: Optional[QuicStreamHandler] = None,
        local_port: int = 0,
    ):
        
        _, _, _, _, address = socket_config
        if len(address) == 2:
            address = ("::ffff:" + address[0], address[1], 0, 0)

        local_host = "::"

        # keep compatibility for Python 3.7 on Windows
        if not hasattr(socket, "IPPROTO_IPV6"):
            socket.IPPROTO_IPV6 = 41

        configuration = QuicConfiguration(
            is_client=True, alpn_protocols=H3_ALPN
        )

        # prepare QUIC connection
        if configuration.server_name is None:
            configuration.server_name = server_name
            
        connection = QuicConnection(
            configuration=configuration, 
            session_ticket_handler=lambda handler: None
        )

        # explicitly enable IPv4/IPv6 dual stack
        self.socket = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        completed = False
        try:
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            self.socket.setblocking(False)
            self.socket.bind((local_host, local_port, 0, 0))
            completed = True
        finally:
            if not completed:
                self.socket.close()
        # connect
        self.loop = asyncio.get_event_loop()
        _, protocol = await self.loop.create_datagram_endpoint(
            lambda: QuicProtocol(
                connection, 
                stream_handler=stream_handler,
                loop=self.loop
            ),
            sock=self.socket,
        )


        protocol.init_connection()

        protocol.connect(address)
        await protocol.wait_connected()

        return protocol
    
    async def close(self):

        try:
            self.transport.close()

        except Exception:
            pass

