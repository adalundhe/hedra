import asyncio
import socket
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
from ssl import SSLContext
from typing import Optional
from asyncio.sslproto import SSLProtocol
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.protocols.shared.reader import Reader
from hedra.core.engines.types.common.protocols.shared.writer import Writer
from .protocol import TCPProtocol
from .tls_protocol import TLSProtocol
from hedra.core.engines.types.common.protocols.shared.constants import (
    _DEFAULT_LIMIT,
    _HTTP2_LIMIT
)


class TCPConnection:

    def __init__(self, factory_type: RequestTypes = RequestTypes.HTTP) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self.factory_type = factory_type
        self._connection = None
        self.socket: socket.socket = None
        self._writer = None

    async def create(self, hostname=None, socket_config=None, *, limit=_DEFAULT_LIMIT, ssl=None):

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

    async def create_http2(self, hostname=None, socket_config=None, ssl: Optional[SSLContext] = None, ssl_timeout: int = SSL_HANDSHAKE_TIMEOUT):
        # this does the same as loop.open_connection(), but TLS upgrade is done
        # manually after connection be established.
        if self.loop is None:
            self.loop = asyncio.get_event_loop()

        family, type_, proto, _, address = socket_config
        
        socket_family = socket.AF_INET6 if family == 2 else socket.AF_INET

        self.socket = socket.socket(family=family, type=type_, proto=proto)
        self.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        await self.loop.run_in_executor(None, self.socket.connect, address)
        
        self.socket.setblocking(False)

        reader = Reader(limit=_HTTP2_LIMIT, loop=self.loop)

        protocol = TLSProtocol(reader, loop=self.loop)

        self.transport, _ = await self.loop.create_connection(
            lambda: protocol, 
            sock=self.socket,
            family=socket_family
        )
        
        ssl_protocol = SSLProtocol(
            self.loop, 
            protocol, 
            ssl, 
            None,
            False, 
            hostname,
            ssl_handshake_timeout=ssl_timeout,
            call_connection_made=False
        )

        # Pause early so that "ssl_protocol.data_received()" doesn't
        # have a chance to get called before "ssl_protocol.connection_made()".
        self.transport.pause_reading()

        self.transport.set_protocol(ssl_protocol)
        await self.loop.run_in_executor(None, ssl_protocol.connection_made, self.transport)
        self.transport.resume_reading()

        self.transport = ssl_protocol._app_transport

        reader = Reader(limit=_HTTP2_LIMIT, loop=self.loop)

        protocol.upgrade_reader(reader) # update reader
        protocol.connection_made(self.transport) # update transport

        self._writer = Writer(self.transport, ssl_protocol, reader, self.loop) # update writer

        return reader, self._writer

    async def close(self):

        try:
            self.transport._ssl_protocol.pause_writing()
            self.transport.close()
            while not self.transport._closed:
                await asyncio.sleep(0)

        except Exception:
            pass