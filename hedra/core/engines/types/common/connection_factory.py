import asyncio
import socket
import weakref
from ssl import SSLContext
from typing import Optional
from asyncio.events import get_running_loop
from asyncio.streams import (
    StreamReader,
    StreamReaderProtocol,
    StreamWriter
)


_DEFAULT_LIMIT= 65536

class TLSStreamReaderProtocol(asyncio.StreamReaderProtocol):

    def upgrade_reader(self, reader: StreamReader):
        if self._stream_reader is not None:
            self._stream_reader.set_exception(Exception('upgraded connection to TLS, this reader is obsolete now.'))
        self._stream_reader_wr = weakref.ref(reader)
        self._source_traceback = reader._source_traceback


class ConnectionFactory:

    def __init__(self) -> None:
      self.loop = None
      self.transport = None

    async def create(self, hostname=None, socket_config=None, *, limit=_DEFAULT_LIMIT, **kwds):

        if self.loop is None:
            self.loop = get_running_loop()

        reader = StreamReader(limit=limit, loop=self.loop)
        protocol = StreamReaderProtocol(reader, loop=self.loop)

        family, type_, proto, _, address = socket_config
        sock = socket.socket(family=family, type=type_, proto=proto)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(False)
        await self.loop.sock_connect(sock, address)

        transport, _ = await self.loop.create_connection(lambda: protocol, sock=sock, server_hostname=hostname, **kwds)
        writer = StreamWriter(transport, protocol, reader, self.loop)

        return reader, writer

    async def create_tls(self, hostname=None, socket_config=None, ssl: Optional[SSLContext] = None):
        # this does the same as loop.open_connection(), but TLS upgrade is done
        # manually after connection be established.
        if self.loop is None:
            self.loop = get_running_loop()

        reader = StreamReader(limit=2**64, loop=self.loop)
        protocol = TLSStreamReaderProtocol(reader, loop=self.loop)

        family, type_, proto, _, address = socket_config
        sock = socket.socket(family=family, type=type_, proto=proto)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.setblocking(False)
        await self.loop.sock_connect(sock, address)

        transport, _ = await self.loop.create_connection(
            lambda: protocol, 
            sock=sock,
            family=socket.AF_INET
        )

        writer = asyncio.StreamWriter(transport, protocol, reader, self.loop)
        # here you can use reader and writer for whatever you want, for example
        # start a proxy connection and start TLS to target host later...
        # now perform TLS upgrade
        if ssl:
            transport = await self.loop.start_tls(
                transport,
                protocol,
                sslcontext=ssl,
                server_side=False,
                server_hostname=hostname
            )

            reader = asyncio.StreamReader(limit=2**64, loop=self.loop)
            protocol.upgrade_reader(reader) # update reader
            protocol.connection_made(transport) # update transport
            writer = asyncio.StreamWriter(transport, protocol, reader, self.loop) # update writer

        return reader, writer
