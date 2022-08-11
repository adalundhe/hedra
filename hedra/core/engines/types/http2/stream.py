import asyncio
from ssl import SSLContext
from typing import Tuple, Optional, Union
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.connection_factory import ConnectionFactory
from .reader_writer import ReaderWriter
from .frames import FrameBuffer
from .frames.types import HeadersFrame, WindowUpdateFrame

class AsyncStream:

    def __init__(self, stream_id: int, timeouts: Timeouts, concurrency: int, reset_connection: bool, stream_type: RequestTypes) -> None:
        self.timeouts = timeouts
        self.connected = False
        self.init_id = stream_id
        self.reset_connection = reset_connection
        self.stream_type = stream_type

        if self.init_id%2 == 0:
            self.init_id += 1

        self.stream_id = 0
        self.concurrency = concurrency
        self.dns_address = None
        self.port = None
        self._connection_factory = ConnectionFactory(stream_type)
        self.lock = asyncio.Lock()
        self.reader_writer = None

        

    async def connect(self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]], 
        ssl: Optional[SSLContext]=None,
        timeout: Optional[float] = None
    ) -> Union[ReaderWriter, Exception]:
        if self.connected is False or self.dns_address != dns_address or self.reset_connection:
            stream = await asyncio.wait_for(
                self._connection_factory.create_http2(
                    hostname,
                    socket_config=socket_config,
                    ssl=ssl
                ),
                timeout=timeout
            )

            self.connected = True
            self.stream_id = self.init_id
            self.dns_address = dns_address
            self.port = port

            reader, writer = stream

            self.reader_writer = ReaderWriter(self.stream_id, reader, writer, self.timeouts)
            self.reader_writer.headers_frame = HeadersFrame(self.init_id)
            self.reader_writer.window_frame = WindowUpdateFrame(self.init_id, window_increment=65536)

        else:

            self.stream_id += 2# self.concurrency
            if self.stream_id%2 == 0:
                self.stream_id += 1

            self.reader_writer.stream_id = self.stream_id
            self.reader_writer.headers_frame.stream_id = self.stream_id
            self.reader_writer.window_frame.stream_id = self.stream_id

        self.reader_writer.frame_buffer = FrameBuffer()
        return self.reader_writer

    async def close(self):
        await self._connection_factory.close()