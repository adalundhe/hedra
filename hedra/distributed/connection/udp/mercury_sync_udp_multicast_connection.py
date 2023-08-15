
from __future__ import annotations
import asyncio
import socket
import zstandard
from dtls import do_patch
from hedra.distributed.connection.udp.protocols import MercurySyncUDPProtocol
from hedra.distributed.env import Env
from typing import (
    Optional,
)
from .mercury_sync_udp_connection import MercurySyncUDPConnection

do_patch()


class MercurySyncUDPMulticastConnection(MercurySyncUDPConnection):
    """Implementation of Zeroconf Multicast DNS Service Discovery
    Supports registration, unregistration, queries and browsing.
    """
    def __init__(
        self, 
        host: str,
        port: int,
        instance_id: int,
        env: Env,
    ):
        super().__init__(
            host,
            port,
            instance_id,
            env
        )

        self._mcast_group = env.MERCURY_SYNC_MULTICAST_GROUP

        if self._mcast_group is None:
            self.group = ('', self.port)

        else:
            self.group = (self._mcast_group, self.port)

    async def connect_async(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None
    ) -> None:
        
        self._loop = asyncio.get_event_loop()
        self._running = True

        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        if worker_socket is None:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except Exception:
                pass

            self.udp_socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 255)
            self.udp_socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)

            try:
                self.udp_socket.bind(self.group)
            except ConnectionRefusedError:
                pass

            except OSError:
                pass
            
            self.udp_socket.setsockopt(
                socket.SOL_IP, 
                socket.IP_MULTICAST_IF, 
                socket.inet_aton(self.host) + socket.inet_aton('0.0.0.0')
            )

            if self._mcast_group is not None:
                self.udp_socket.setsockopt(
                    socket.SOL_IP, 
                    socket.IP_ADD_MEMBERSHIP, 
                    socket.inet_aton(self.udp_socket) + socket.inet_aton('0.0.0.0')
                )

            self.udp_socket.setblocking(False)

        else:
            self.udp_socket = worker_socket


        if cert_path and key_path:
            self._udp_ssl_context = self._create_udp_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
            )

            self.udp_socket = self._udp_ssl_context.wrap_socket(self.udp_socket)

        server = self._loop.create_datagram_endpoint(
            lambda: MercurySyncUDPProtocol(
                self.read
            ),
            sock=self.udp_socket
        )

        transport, _ = await server
        self._transport = transport

        self._cleanup_task = self._loop.create_task(self._cleanup())
