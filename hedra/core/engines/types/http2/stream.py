
import h2.settings
import struct
from hedra.core.engines.types.common.encoder import Encoder
from hedra.core.engines.types.common.protocols.shared.reader import Reader
from hedra.core.engines.types.common.protocols.shared.writer import Writer
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.windows.window_manager import WindowManager


class Stream:
    READ_NUM_BYTES=65536

    __slots__ = (
        'stream_id',
        'reader',
        'writer',
        'timeouts',
        'max_inbound_frame_size',
        'max_outbound_frame_size',
        'current_outbound_window_size',
        'content_length',
        'expected_content_length',
        'reset_connection',
        'inbound',
        'outbound',
        'closed_by',
        '_authority',
        'frame_buffer',
        'encoder',
        '_remote_settings',
        '_remote_settings_dict',
        'settings_frame',
        'headers_frame',
        'window_frame',
        'connection_data',
        '_STRUCT_HBBBL',
        '_STRUCT_LL',
        '_STRUCT_HL',
        '_STRUCT_LB',
        '_STRUCT_L',
        '_STRUCT_H',
        '_STRUCT_B'
    )

    def __init__(self, stream_id: int, timeouts: Timeouts) -> None:
        self.stream_id = stream_id
        self.reader: Reader = None
        self.writer: Writer = None
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
        self._remote_settings = h2.settings.Settings(
            client=False
        )
        self._remote_settings_dict = {
            setting_name: setting_value for setting_name, setting_value in self._remote_settings.items()
        }

        self.settings_frame = None
        self.headers_frame = None
        self.window_frame = None

        self.connection_data = bytearray(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n')

        self._STRUCT_HBBBL = struct.Struct(">HBBBL")
        self._STRUCT_LL = struct.Struct(">LL")
        self._STRUCT_HL = struct.Struct(">HL")
        self._STRUCT_LB = struct.Struct(">LB")
        self._STRUCT_L = struct.Struct(">L")
        self._STRUCT_H = struct.Struct(">H")
        self._STRUCT_B = struct.Struct(">B")

    def write(self, data: bytes):
        self.writer._transport.write(data)

    def read(self, msg_length: int=READ_NUM_BYTES):
        return self.reader.read(msg_length)

    def get_raw_buffer(self) -> bytearray:
        return self.reader._buffer

    def write_window_update_frame(self, stream_id: int=None, window_increment: int=None):

        if stream_id is None:
            stream_id = self.stream_id

        body = self._STRUCT_L.pack(window_increment & 0x7FFFFFFF)
        body_len = len(body)

        type = 0x08

        # Build the common frame header.
        # First, get the flags.
        flags = 0

        header = self._STRUCT_HBBBL.pack(
            (body_len >> 8) & 0xFFFF,  # Length spread over top 24 bits
            body_len & 0xFF,
            type,
            flags,
            stream_id & 0x7FFFFFFF  # Stream ID is 32 bits.
        )

        self.writer.write(header + body)