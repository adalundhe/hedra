import asyncio
from asyncio import StreamReader, StreamWriter
from typing import Tuple, Optional, Union
from ssl import SSLContext
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.common.connection_factory import ConnectionFactory


class AsyncStream:
    READ_NUM_BYTES=65536

    def __init__(self, stream_id: int, timeouts: Timeouts, concurrency: int, reset_connection: bool) -> None:
        self.reader: StreamReader = None
        self.writer: StreamWriter = None
        self.timeouts = timeouts
        self.connected = False
        self.init_id = stream_id
        self.reset_connection = reset_connection

        if self.init_id%2 == 0:
            self.init_id += 1

        self.stream_id = 0
        self.concurrency = concurrency
        self.dns_address = None
        self.port = None
        self._connection_factory = ConnectionFactory()

    async def connect(self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]], 
        ssl: Optional[SSLContext]=None
    ) -> Union[Tuple[StreamReader, StreamWriter], Exception]:
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            stream = await self._connection_factory.create_tls(
                hostname,
                dns_address=dns_address,
                port=port,
                ssl=ssl
            )
            self.reader, self.writer = stream
            self.connected = True
            self.stream_id = self.init_id
            self.dns_address = dns_address
            self.port = port

        else:
            self.stream_id += self.concurrency
            if self.stream_id%2 == 0:
                self.stream_id += 1

    def write(self, data: bytes):
        self.writer.write(data)

    async def read(self, msg_length: int=READ_NUM_BYTES):
        return await asyncio.wait_for(self.reader.read(msg_length), self.timeouts.total_timeout)
