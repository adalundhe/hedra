from typing import Optional, Dict, Any
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
    #: The byte that signals the SETTINGS_HEADER_TABLE_SIZE setting.
    HEADER_TABLE_SIZE = 0x01
    #: The byte that signals the SETTINGS_ENABLE_PUSH setting.
    ENABLE_PUSH = 0x02
    #: The byte that signals the SETTINGS_MAX_CONCURRENT_STREAMS setting.
    MAX_CONCURRENT_STREAMS = 0x03
    #: The byte that signals the SETTINGS_INITIAL_WINDOW_SIZE setting.
    INITIAL_WINDOW_SIZE = 0x04
    #: The byte that signals the SETTINGS_MAX_FRAME_SIZE setting.
    MAX_FRAME_SIZE = 0x05
    #: The byte that signals the SETTINGS_MAX_HEADER_LIST_SIZE setting.
    MAX_HEADER_LIST_SIZE = 0x06
    #: The byte that signals SETTINGS_ENABLE_CONNECT_PROTOCOL setting.
    ENABLE_CONNECT_PROTOCOL = 0x08

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
        if 'ACK' in self.flags and len(data) > 0:
            raise Exception(
                "SETTINGS ack frame must not have payload: got %s bytes" %
                len(data)
            )

        body_len = 0
        for i in range(0, len(data), 6):

            name, value = _STRUCT_HL.unpack(data[i:i+6])

            self.settings[name] = value
            body_len += 6

        self.body_len = body_len