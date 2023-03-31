from __future__ import annotations
import asyncio
from typing import Optional, Tuple
from hedra.core.engines.types.common.protocols import UDPConnection
from hedra.core.engines.types.common.protocols.udp.quic_protocol import QuicProtocol


class HTTP3Connection:

    __slots__ = (
        'dns_address',
        'port',
        'ip_addr',
        'lock',
        'protocol',
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

        self.protocol: QuicProtocol = None
        self.connected = False
        self.reset_connection = reset_connection
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
