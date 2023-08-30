import asyncio
from typing import Callable, Tuple


class MercurySyncTCPServerProtocol(asyncio.Protocol):
    def __init__(
        self, 
        callback: Callable[
            [
                bytes,
                Tuple[str, int]
            ],
            bytes
        ]
    ):
        super().__init__()
        self.callback = callback
        self.transport: asyncio.Transport = None
        self.loop = asyncio.get_event_loop()
        self.on_con_lost = self.loop.create_future()


    def connection_made(self, transport) -> str:
        self.transport = transport

    def data_received(self, data: bytes):
        self.callback(
            data,
            self.transport
        )

    def connection_lost(self, exc: Exception | None) -> None:
        self.on_con_lost.set_result(True)