import asyncio
import socket
from typing import Callable

from hedra.core.engines.types.common.protocols.shared.constants import _DEFAULT_LIMIT
from hedra.core.engines.types.common.protocols.shared.reader import Reader
from hedra.core.engines.types.common.protocols.shared.writer import Writer
from hedra.core.engines.types.common.types import RequestTypes

from .protocol import UDPProtocol

QuicStreamHandler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], None]


class UDPConnection:

    def __init__(self, factory_type: RequestTypes = RequestTypes.HTTP) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport: asyncio.DatagramTransport = None
        self.factory_type = factory_type
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

    async def create_udp(self, socket_config=None, *, limit=_DEFAULT_LIMIT, tls=None):
        
        self.loop = asyncio.get_event_loop()

        family, type_, _, _, address = socket_config

        self.socket = socket.socket(family=family, type=type_)
        
        await self.loop.run_in_executor(None, self.socket.connect, address)

        self.socket.setblocking(False)

        reader = Reader(limit=limit, loop=self.loop)
        reader_protocol = UDPProtocol(reader, loop=self.loop)

        self.transport, _ = await self.loop.create_datagram_endpoint(
            lambda: reader_protocol, 
            sock=self.socket
        )

        # TODO: Enable DTLS - This appears to be a *significant* amount of work
        # but would allow for VOIP testing, etc.

        self._writer = Writer(self.transport, reader_protocol, reader, self.loop)
        
        return reader, self._writer
    
    async def close(self):

        try:
            self.transport.close()

        except Exception:
            pass

