import asyncio
from asyncio.constants import SSL_HANDSHAKE_TIMEOUT
import socket
from weakref import ref
from ssl import SSLContext
from typing import Optional
from .fast_streams import (
    FastReader,
    FastWriter,
    FastReaderProtocol
)
from asyncio.sslproto import SSLProtocol
from .types import RequestTypes

_SSL_CONNECT_TIMEOUT = 30
_DEFAULT_LIMIT= 65536
_HTTP2_LIMIT = _DEFAULT_LIMIT * 1024


class TLSStreamReaderProtocol(asyncio.StreamReaderProtocol):

    def upgrade_reader(self, reader: FastReader):

        if self._stream_reader:
            self._stream_reader.set_exception(Exception('upgraded connection to TLS, this reader is obsolete now.'))

        self._stream_reader_wr = ref(reader)
        self._source_traceback = reader._source_traceback


class Connection:

    def __init__(self, reader: FastReader, writer: FastWriter) -> None:
        self._reader = reader
        self._writer = writer

    def send(self, data: bytes):
        return self._writer.write(data)

    async def read(self, size: int=_DEFAULT_LIMIT):
        return await self._reader.read(size)

    async def readuntil(self, sep=b'\n'):
        return await self._reader.readuntil(separator=sep)

class ConnectionFactory:

    def __init__(self, factory_type: RequestTypes = RequestTypes.HTTP) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()
        self.transport = None
        self.factory_type = factory_type
        self._connection = None

    async def create(self, hostname=None, socket_config=None, *, limit=_DEFAULT_LIMIT, ssl=None):
        
        family, type_, proto, _, address = socket_config

        sock = socket.socket(family=family, type=type_, proto=proto)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        await self.loop.run_in_executor(None, sock.connect, address)

        sock.setblocking(False)

        reader = FastReader(limit=limit, loop=self.loop)
        reader_protocol = FastReaderProtocol(reader, loop=self.loop)

        if ssl is None:
            hostname = None

        self.transport, _ = await self.loop.create_connection(
            lambda: reader_protocol, 
            sock=sock,
            server_hostname=hostname,
            ssl=ssl
        )

        writer = FastWriter(self.transport, reader_protocol, reader, self.loop)

        self._connection = Connection(reader, writer)
        
        return self._connection

    async def create_http2(self, hostname=None, socket_config=None, ssl: Optional[SSLContext] = None, ssl_timeout: int = SSL_HANDSHAKE_TIMEOUT):
        # this does the same as loop.open_connection(), but TLS upgrade is done
        # manually after connection be established.
        if self.loop is None:
            self.loop = asyncio.get_event_loop()

        family, type_, proto, _, address = socket_config
        sock = socket.socket(family=family, type=type_, proto=proto)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        await self.loop.run_in_executor(None, sock.connect, address)
        sock.setblocking(False)

        reader = FastReader(limit=_HTTP2_LIMIT, loop=self.loop)

        if self.factory_type == RequestTypes.GRPC:
            protocol = TLSStreamReaderProtocol(reader, loop=self.loop)

            self.transport, _ = await self.loop.create_connection(
                lambda: protocol, 
                sock=sock,
                happy_eyeballs_delay=0.1,
                family=socket.AF_INET
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

            reader = FastReader(limit=_HTTP2_LIMIT, loop=self.loop)
            protocol.upgrade_reader(reader) # update reader
            protocol.connection_made(self.transport) # update transport
            writer = FastWriter(self.transport, ssl_protocol, reader, self.loop) # update writer


            return reader, writer

        else:
            reader_protocol = FastReaderProtocol(reader, loop=self.loop)

            if ssl is None:
                hostname = None

            self.transport, _ = await self.loop.create_connection(
                lambda: reader_protocol, 
                sock=sock,
                server_hostname=hostname,
                ssl=ssl
            )

            writer = FastWriter(self.transport, reader_protocol, reader, self.loop)

            return reader, writer