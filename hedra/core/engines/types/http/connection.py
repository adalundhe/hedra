from __future__ import annotations
import asyncio
from ssl import SSLContext
from typing import Optional, Tuple
from hedra.core.engines.types.common.protocols import (
    TCPConnection,
    TCPReader,
    TCPWriter
)

from hedra.core.engines.types.common.protocols.tcp.constants import _DEFAULT_LIMIT


class HTTPConnection:

    def __init__(self, reset_connection: bool=False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.reader: TCPReader = None
        self.writer: TCPWriter = None

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
        timeout: Optional[float]=None
    ) -> None:
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            try:
                reader, writer = await asyncio.wait_for(self._connection_factory.create(hostname, socket_config, ssl=ssl), timeout=timeout)
                self.connected = True

                self.reader = reader
                self.writer = writer

                self.dns_address = dns_address
                self.port = port
                self.ssl = ssl

            except asyncio.TimeoutError:
                raise Exception('Connection timed out.')

            except ConnectionResetError:
                raise Exception('Connection reset.')

            except Exception as e:
                raise e

    @property
    def empty(self):
        return not self.reader._buffer

    def read(self):
        return self.reader.read(n=_DEFAULT_LIMIT)

    def readexactly(self, n_bytes: int):
        return self.reader.read(n=n_bytes)

    def readuntil(self, sep=b'\n'):
        return self.reader.readuntil(separator=sep)

    def write(self, data):
        self.writer.write(data)

    def reset_buffer(self):
        self.reader._buffer = bytearray()

    def read_headers(self):
        return self.reader.read_headers()

    async def close(self):
        try:
            await self._connection_factory.close()
        except Exception:
            pass