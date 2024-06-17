import asyncio
import socket

from hedra.core_rewrite.engines.client.shared.protocols import (
    _DEFAULT_LIMIT,
    Reader,
    Writer,
)

from .protocol import TCPProtocol


class TCPConnection:

    def __init__(self) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

    async def create(
            self, 
            hostname=None, 
            socket_config=None, 
            *, 
            limit=_DEFAULT_LIMIT, 
            ssl=None
    ):
        self.loop = asyncio.get_event_loop()

        family, type_, proto, _, address = socket_config

        socket_family = socket.AF_INET6 if family == 2 else socket.AF_INET

        self.socket = socket.socket(family=family, type=type_, proto=proto)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        
        await self.loop.run_in_executor(None, self.socket.connect, address)

        self.socket.setblocking(False)

        reader = Reader(limit=limit, loop=self.loop)
        reader_protocol = TCPProtocol(reader, loop=self.loop)

        if ssl is None:
            hostname = None

        self.transport, _ = await self.loop.create_connection(
            lambda: reader_protocol, 
            sock=self.socket,
            family=socket_family,
            server_hostname=hostname,
            ssl=ssl
        )

        self._writer = Writer(self.transport, reader_protocol, reader, self.loop)

        return reader, self._writer

    async def close(self):

        try:
            self.transport._ssl_protocol.pause_writing()
            self.transport.close()

        except Exception:
            pass