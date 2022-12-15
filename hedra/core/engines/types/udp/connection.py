from __future__ import annotations
import asyncio
from typing import Optional, Tuple
from hedra.core.engines.types.common.protocols import UDPConnection as UDP
from hedra.core.engines.types.common.protocols.shared.reader import Reader
from hedra.core.engines.types.common.protocols.shared.writer import Writer
from hedra.core.engines.types.common.protocols.shared.constants import _DEFAULT_LIMIT


class UDPConnection:

    __slots__ = (
        'dns_address',
        'port',
        'ip_addr',
        'lock',
        'reader',
        'writer',
        'connected',
        'reset_connection',
        'pending',
        '_connection_factory'
    )

    def __init__(self, reset_connection: bool=False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.reader: Reader = None
        self.writer: Writer = None

        self.connected = False
        self.reset_connection = reset_connection
        self.pending = 0
        self._connection_factory = UDP()

    async def make_connection(
        self, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        timeout: Optional[float]=None
    ) -> None:
    
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            try:
                reader, writer = await asyncio.wait_for(self._connection_factory.create(socket_config), timeout=timeout)
                self.connected = True

                self.reader = reader
                self.writer = writer

                self.dns_address = dns_address
                self.port = port

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
        self.writer.send(data)

    def reset_buffer(self):
        self.reader._buffer = bytearray()

    def read_headers(self):
        return self.reader.read_headers()

    async def close(self):
        try:
            await self._connection_factory.close()
        except Exception:
            pass