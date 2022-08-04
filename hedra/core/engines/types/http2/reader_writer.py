import asyncio
from hedra.core.engines.types.common.encoder import Encoder
from hedra.core.engines.types.common.fast_streams import FastReader, FastWriter
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.windows.window_manager import WindowManager


class ReaderWriter:
    READ_NUM_BYTES=65536

    def __init__(self, stream_id: int, reader: FastReader, writer: FastWriter, timeouts: Timeouts) -> None:
        self.stream_id = stream_id
        self.reader= reader
        self.writer = writer
        self.timeouts = timeouts
        self.max_inbound_frame_size = 0
        self.max_outbound_frame_size = 0
        self.current_outbound_window_size = 0
        self.content_length = 0
        self.expected_content_length = 0
        self.reset_connection = False
        self.inbound: WindowManager  = None
        self.outbound: WindowManager = None
        self.closed_by = None
        self._authority = None
        self.frame_buffer = None
        self.encoder: Encoder = None

    def write(self, data: bytes):
        self.writer._transport.write(data)

    async def read(self, msg_length: int=READ_NUM_BYTES):
        return await self.reader.read(msg_length)

    def get_raw_buffer(self) -> bytearray:
        return self.reader._buffer