from typing import Optional, Dict, Any
from hedra.core.engines.types.http2.events.remote_settings_changed_event import RemoteSettingsChanged
from hedra.core.engines.types.http2.events.settings_acknowledged_event import SettingsAcknowledged
from hedra.core.engines.types.http2.stream import Stream
from hedra.core.engines.types.http2.streams.stream_settings import SettingCodes
from .base_frame import Frame
from .attributes import (
    Flag,
    _STREAM_ASSOC_NO_STREAM,
    _STRUCT_HL
)


class SettingsFrame(Frame):
    frame_type='SETTINGS'
    """
    The SETTINGS frame conveys configuration parameters that affect how
    endpoints communicate. The parameters are either constraints on peer
    behavior or preferences.

    Settings are not negotiated. Settings describe characteristics of the
    sending peer, which are used by the receiving peer. Different values for
    the same setting can be advertised by each peer. For example, a client
    might set a high initial flow control window, whereas a server might set a
    lower value to conserve resources.
    """
    #: The flags defined for SETTINGS frames.
    defined_flags = [Flag('ACK', 0x01)]

    #: The type byte defined for SETTINGS frames.
    type = 0x04

    stream_association = _STREAM_ASSOC_NO_STREAM

    # We need to define the known settings, they may as well be class
    # attributes.

    def __init__(self, stream_id: int = 0, settings: Optional[Dict[int, int]] = None, **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        if settings and "ACK" in kwargs.get("flags", ()):
            raise Exception(
                "Settings must be empty if ACK flag is set."
            )

        #: A dictionary of the setting type byte to the value of the setting.
        self.settings = settings or {}

    def _body_repr(self) -> str:
        return f"settings={self.settings}"

    def serialize_body(self) -> bytes:
        body = bytearray()
        for setting, value in self.settings.items():
            body.extend(_STRUCT_HL.pack(setting & 0xFF, value))
        return body

    def parse_body(self, data: bytearray) -> None:
        body_len = 0
        for i in range(0, len(data), 6):

            name, value = _STRUCT_HL.unpack(data[i:i+6])

            self.settings[name] = value
            body_len += 6

        self.body_len = body_len

    def get_events_and_frames(self, stream: Stream, connection):
        events = []
        if 'ACK' in self.flags:

            changes = connection.local_settings.acknowledge()
    
            initial_window_size_change = changes.get(SettingCodes.INITIAL_WINDOW_SIZE)
            max_header_list_size_change = changes.get(SettingCodes.MAX_HEADER_LIST_SIZE)
            max_frame_size_change = changes.get(SettingCodes.MAX_FRAME_SIZE)
            header_table_size_change =changes.get(SettingCodes.HEADER_TABLE_SIZE)

            if initial_window_size_change is not None:

                window_delta = initial_window_size_change.new_value - initial_window_size_change.original_value
                
                new_max_window_size = stream.inbound.max_window_size + window_delta
                stream.inbound.window_opened(window_delta)
                stream.inbound.max_window_size = new_max_window_size

            if max_header_list_size_change is not None:
                connection._decoder.max_header_list_size = max_header_list_size_change.new_value

            if max_frame_size_change is not None:
                stream.max_outbound_frame_size =  max_frame_size_change.new_value

            if header_table_size_change:
                # This is safe across all hpack versions: some versions just won't
                # respect it.
                connection._decoder.max_allowed_table_size = header_table_size_change.new_value

            ack_event = SettingsAcknowledged()
            ack_event.changed_settings = changes
            events.append(ack_event)
            return [], events

        # Add the new settings.
        connection.remote_settings.update(self.settings)
        events.append(
            RemoteSettingsChanged.from_settings(
                connection.remote_settings, self.settings
            )
        )

        changes = connection.remote_settings.acknowledge()
        initial_window_size_change = changes.get(SettingCodes.INITIAL_WINDOW_SIZE)
        header_table_size_change = changes.get(SettingCodes.HEADER_TABLE_SIZE)
        max_frame_size_change = changes.get(SettingCodes.MAX_FRAME_SIZE)
     
        if initial_window_size_change:
            stream.current_outbound_window_size = connection._guard_increment_window(
                stream.current_outbound_window_size,
                initial_window_size_change.new_value - initial_window_size_change.original_value
            )

        # HEADER_TABLE_SIZE changes by the remote part affect our encoder: cf.
        # RFC 7540 Section 6.5.2.
        if  header_table_size_change:
            connection._encoder.header_table_size = header_table_size_change.new_value

        if max_frame_size_change:
            stream.max_outbound_frame_size = max_frame_size_change.new_value

        frames = SettingsFrame(0)
        frames.flags.add('ACK')

        return [frames], events