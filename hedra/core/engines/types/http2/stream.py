import asyncio
from ssl import SSLContext
from typing import Tuple, Optional, Union
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.connection_factory import ConnectionFactory
from hedra.core.engines.types.common.types import RequestTypes
from .reader_writer import ReaderWriter


class AsyncStream:

    def __init__(self, stream_id: int, timeouts: Timeouts, concurrency: int, reset_connection: bool, stream_type: RequestTypes) -> None:
        self.timeouts = timeouts
        self.connected = False
        self.reset_connection = reset_connection
        self.stream_type = stream_type
        
        self.init_id = stream_id

        if self.init_id%2 == 0:
            self.init_id += 1

        self.stream_id = 0
        self.concurrency = concurrency
        self.dns_address = None
        self.port = None
        self._connection_factory = ConnectionFactory(stream_type)
        self.lock = asyncio.Lock()
        self.reader_writer: ReaderWriter = None

    async def connect(self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]], 
        ssl: Optional[SSLContext]=None
    ) -> Union[ReaderWriter, Exception]:
        try:
            if self.connected is False or self.dns_address != dns_address or self.reset_connection:
                stream = await self._connection_factory.create_http2(
                    hostname,
                    socket_config=socket_config,
                    ssl=ssl
                )
                self.connected = True
                self.stream_id = self.init_id
                self.dns_address = dns_address
                self.port = port

                reader, writer = stream

                self.reader_writer = ReaderWriter(self.stream_id, reader, writer, self.timeouts)

            else:

                self.stream_id += 2# self.concurrency
                if self.stream_id%2 == 0:
                    self.stream_id += 1

                self.reader_writer.stream_id = self.stream_id

            return self.reader_writer

        except Exception as e:
            raise e

