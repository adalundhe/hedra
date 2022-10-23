import asyncio
import traceback
import h2.settings
from ssl import SSLContext
from typing import Tuple, Optional, Union
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.protocols.tcp import TCPConnection
from .stream import Stream
from .frames import FrameBuffer
from .frames.types.base_frame import Frame

class HTTP2Connection:

    __slots__ = (
        'timeouts',
        'connected',
        'init_id',
        'reset_connection',
        'stream_type',
        'init_id',
        'stream_id',
        'concurrency',
        'dns_address',
        'port',
        'connection',
        'lock',
        'stream',
        'local_settings',
        'remote_settings',
        'outbound_flow_control_window',
        'local_settings_dict',
        'remote_settings_dict',
        'settings_frame',
        'headers_frame',
        'window_update_frame'
    )

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

        self.connection = TCPConnection(stream_type)
        self.lock = asyncio.Lock()
        self.stream = Stream(self.stream_id, self.timeouts)

        self.local_settings = h2.settings.Settings(
            client=True,
            initial_values={
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: concurrency,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 65535,
            }
        )
        self.remote_settings = h2.settings.Settings(
            client=False
        )

        self.outbound_flow_control_window = self.remote_settings.initial_window_size

        del self.local_settings[h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL]

        self.local_settings_dict = {setting_name: setting_value for setting_name, setting_value in self.local_settings.items()}
        self.remote_settings_dict = {setting_name: setting_value for setting_name, setting_value in self.remote_settings.items()}

        self.settings_frame = Frame(0, 0x04, settings=self.local_settings_dict)
        self.headers_frame = Frame(self.init_id, 0x01)
        self.headers_frame.flags.add('END_HEADERS')
        
        self.window_update_frame = Frame(self.init_id, 0x08, window_increment=65536)

        self.stream.connection_data.extend(self.settings_frame.serialize())

    async def connect(self, 
        hostname: str, 
        dns_address: str,
        port: int, 
        socket_config: Tuple[int, int, int, int, Tuple[int, int]], 
        ssl: Optional[SSLContext]=None,
        timeout: Optional[float] = None
    ) -> Union[Stream, Exception]:
        
            try:
                if self.connected is False or self.dns_address != dns_address or self.reset_connection:
                    reader, writer = await asyncio.wait_for(
                        self.connection.create_http2(
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

                    self.stream.reader = reader
                    self.stream.writer = writer
                  
                    self.stream.headers_frame = self.headers_frame
                    self.stream.window_frame = self.window_update_frame

                else:

                    self.stream_id += 2# self.concurrency
                    if self.stream_id%2 == 0:
                        self.stream_id += 1

                    self.stream.stream_id = self.stream_id
                    self.stream.headers_frame.stream_id = self.stream_id
                    self.stream.window_frame.stream_id = self.stream_id

                self.stream.frame_buffer = FrameBuffer()
                return self.stream
            
            except asyncio.TimeoutError:
                raise Exception('Connection timed out.')

            except ConnectionResetError:
                raise Exception('Connection reset.')

            except Exception as e:
                raise e

    async def close(self):
        await self.connection.close()