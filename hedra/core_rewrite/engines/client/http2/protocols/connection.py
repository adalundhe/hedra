from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Optional, Tuple

from hedra.core_rewrite.engines.client.http2.streams import Stream

from .shared import _DEFAULT_LIMIT
from .tcp import TCPConnection


class HTTP2Connection:

    def __init__(
        self,  
        concurrency: int,
        stream_id: int=1,
        reset_connection: bool=False
    ) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.stream_id = stream_id
        self.lock = asyncio.Lock()

        self.stream = Stream(
            concurrency, 
            stream_id=stream_id, 
            reset_connection=reset_connection,
        )

        self.connected = False
        self.reset_connection = reset_connection
        self.pending = 0
        self._connection_factory = TCPConnection()

    async def make_connection(
        self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        ssl: Optional[SSLContext]=None,
        timeout: Optional[float]=None,
        ssl_upgrade: bool =False
    ):
        if self.connected is False or self.dns_address != dns_address  or ssl_upgrade:
            
            reader, writer = await self._connection_factory.create_http2(hostname, socket_config, ssl=ssl)

            self.stream.reader = reader
            self.stream.writer = writer

            self.connected = True
            self.dns_address = dns_address
            self.port = port
            self.ssl = ssl
        else:
            self.stream.update_stream_id()

    @property
    def empty(self):
        return not self.stream.reader._buffer

    def read(self):
        return self.stream.reader.read(n=_DEFAULT_LIMIT)

    def readexactly(self, n_bytes: int):
        return self.stream.reader.readexactly(n=n_bytes)

    def readuntil(self, sep=b'\n'):
        return self.stream.reader.readuntil(separator=sep)
    
    def readline(self):
        return self.stream.reader.readline()

    def write(self, data):
        self.stream.writer.write(data)

    def reset_buffer(self):
        self.stream.reader._buffer = bytearray()

    def read_headers(self):
        return self.stream.reader.read_headers()

    async def close(self):
        try:
            await self._connection_factory.close()
        except Exception:
            pass