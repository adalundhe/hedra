from __future__ import annotations

import asyncio
from typing import Optional, Tuple

from .quic_protocol import QuicProtocol
from .udp_connection import UDPConnection


class HTTP3Connection:

    def __init__(self, reset_connections: bool=False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.protocol: Optional[QuicProtocol] = None
        self.connected = False
        self.reset_connections = reset_connections
        self.pending = 0
        self._connection_factory = UDPConnection()

    async def make_connection(
        self, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        server_name: str=None,
        timeout: Optional[float]=None
    ) -> None:
    
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            try:
                self.protocol = await asyncio.wait_for(
                    self._connection_factory.create_http3(
                        socket_config=socket_config,
                        server_name=server_name
                    ),
                     
                    timeout=timeout
                )
                    
                self.connected = True

                self.dns_address = dns_address
                self.port = port

            except asyncio.TimeoutError:
                raise Exception('Connection timed out.')

            except ConnectionResetError:
                raise Exception('Connection reset.')

            except Exception as e:
                raise e

    async def close(self):
        if self.protocol:
            self.protocol.close()
