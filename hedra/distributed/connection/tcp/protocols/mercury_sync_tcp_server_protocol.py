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

    def connection_made(self, transport) -> str:
        self.transport = transport

    def data_received(self, data: bytes):
        self.callback(
            data,
            self.transport
        )