import asyncio
from typing import Callable, Any


class MercurySyncTCPClientProtocol(asyncio.Protocol):
    def __init__(
        self, 
        callback: Callable[
            [Any],
            bytes
        ]
    ):
        super().__init__()
        self.transport: asyncio.Transport = None
        self.loop = asyncio.get_event_loop()
        self.callback = callback

        self.on_con_lost = self.loop.create_future()

    def connection_made(self, transport: asyncio.Transport) -> str:
        self.transport = transport

    def data_received(self, data: bytes):
        self.callback(
            data,
            self.transport
        )

    def connection_lost(self, exc):
        self.on_con_lost.set_result(True)
