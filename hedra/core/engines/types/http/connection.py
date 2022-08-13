from __future__ import annotations
import asyncio
from ssl import SSLContext
from typing import Optional, Tuple, Union
from hedra.core.engines.types.common.connection_factory import (
    ConnectionFactory, 
    Connection
)


class HTTPConnection:

    def __init__(self, reset_connection: bool=False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ssl: SSLContext = None
        self.ip_addr = None
        self.lock = asyncio.Lock()
        self._connection: Connection = None
        self.connected = False
        self.reset_connection = reset_connection
        self.pending = 0
        self._connection_factory = ConnectionFactory()

    async def make_connection(
        self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        ssl: Optional[SSLContext]=None,
        timeout: Optional[float]=None
    ) -> Connection:
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            try:
                self._connection = await asyncio.wait_for(self._connection_factory.create(hostname, socket_config, ssl=ssl), timeout=timeout)
                self.connected = True

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
        return not self._connection._reader._buffer

    def read(self):
        return self._connection.read()

    def readexactly(self, n_bytes: int):
        return self._connection.read(n_bytes)

    def readuntil(self, sep=b'\n'):
        return self._connection.readuntil(sep=sep)

    def write(self, data):
        self._connection.send(data)

    def reset_buffer(self):
        self._connection._reader._buffer = bytearray()

    def read_headers(self):
        return self._connection.read_headers()

    async def close(self):
        try:
            await self._connection_factory.close()
        except Exception:
            pass