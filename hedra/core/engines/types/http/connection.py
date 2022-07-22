from __future__ import annotations
import asyncio
from ssl import SSLContext
from typing import Optional, Tuple, Union
from hedra.core.engines.types.common.connection_factory import (
    ConnectionFactory, 
    Connection
)


class Connection:

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
        timeout: int=None, 
        ssl: Optional[SSLContext]=None
    ) -> Connection:
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            try:
                self._connection = await asyncio.wait_for(self._connection_factory.create(hostname, socket_config, ssl=ssl), timeout)
                self.connected = True

                self.dns_address = dns_address
                self.port = port
                self.ssl = ssl

            except asyncio.TimeoutError:
                raise Exception('Connection timed out.')

            except Exception as e:
                raise e

    @property
    def empty(self):
        return not self._connection._reader._buffer

    async def read(self):
        return await self._connection.read()

    async def readexactly(self, n_bytes: int):
        return await self._connection._reader.read(n_bytes)

    async def readuntil(self, sep=b'\n'):
        return await self._connection._reader.readuntil(separator=sep)

    def write(self, data):
        self._connection.send(data)

    def reset_buffer(self):
        self._connection._reader._buffer = bytearray()

    async def close(self):
        try:
            self._connection_factory.transport.close()
            
            while not self._connection_factory.transport.is_closing():
                await asyncio.sleep(0.1)

        except Exception:
            pass