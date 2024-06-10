from __future__ import annotations

import asyncio
import ssl
from typing import Optional, Tuple

from hedra.core_rewrite.engines.client.shared.protocols import (
    Reader,
    Writer,
)

from .dtls import do_patch
from .udp import UDPConnection as UDP

do_patch()

class UDPConnection:

    def __init__(self, reset_connections: bool=False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.reader: Reader = None
        self.writer: Writer = None

        self.connected = False
        self.reset_connections = reset_connections
        self.pending = 0
        self._connection_factory = UDP()

    async def make_connection(
        self, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        tls: Optional[ssl.SSLContext]=None
    ) -> None:
    
        if self.connected is False or self.dns_address != dns_address or self.reset_connections:
            try:
                reader, writer = self._connection_factory.create_udp(
                    socket_config,
                    tls=tls
                )
                    
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

    async def close(self):
        try:
            await self._connection_factory.close()
        except Exception:
            pass