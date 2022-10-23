# -*- coding: utf-8 -*-
"""
hyperframe/frame
~~~~~~~~~~~~~~~~

Defines framing logic for HTTP/2. Provides both classes to represent framed
data and logic for aiding the connection when it comes to reading from the
socket.
"""
import h2.errors
import sys
import struct
from typing import Any, Optional,  List, Iterable
from hedra.core.engines.types.http2.stream import Stream
from hedra.core.engines.types.http2.events.remote_settings_changed_event import RemoteSettingsChanged
from hedra.core.engines.types.http2.events.settings_acknowledged_event import SettingsAcknowledged
from hedra.core.engines.types.http2.streams.stream_settings import SettingCodes
from hedra.core.engines.types.http2.events.stream_reset import StreamReset
from hedra.core.engines.types.http2.streams.stream_closed_by import StreamClosedBy
from hedra.core.engines.types.http2.events.deferred_headers_event import DeferredHeaders
from hedra.core.engines.types.http2.events.connection_terminated_event import ConnectionTerminated
from hedra.core.engines.types.http2.errors.exceptions import StreamClosedError
from hedra.core.engines.types.http2.events.data_received_event import DataReceived
from hedra.core.engines.types.http2.events.stream_ended_event import StreamEnded
from hedra.core.engines.types.http2.errors.exceptions import StreamError
from hedra.core.engines.types.http2.errors.types import ErrorCodes
from hedra.core.engines.types.http2.events.window_updated_event import WindowUpdated

from .attributes import (
    Flag,
    Flags,
    _STRUCT_HBBBL,
    _STRUCT_H,
    _STRUCT_B,
    _STRUCT_LL,
    _STRUCT_LB,
    _STRUCT_L,
    _STRUCT_HL
)
from .utils import raw_data_repr


class Frame:
    __slots__ = (
        'stream_id', 
        'flags',
        'body_len',
        'flags',
        'type',
        'frame_type',
        'data',
        'settings',
        'origin',
        'field',
        'error_code',
        'pad_length',
        'last_stream_id',
        'additional_data',
        'depends_on',
        'stream_weight',
        'exclusive',
        'opaque_data',
        'promised_stream_id',
        'window_increment',
        'flag_byte',
        'defined_flags'
    )
    
    FRAMES = {}
    """
    The base class for all HTTP/2 frames.
    """
    #: The flags defined on this type of frame.

    # If 'has-stream', the frame's stream_id must be non-zero. If 'no-stream',
    # it must be zero. If 'either', it's not checked.
    stream_association: Optional[str] = None
    frame_types = {
        0xA: sys.intern('ALTSVC'),
        0x09: sys.intern('CONTINUATION'),
        0x0: sys.intern('DATA'),
        0x07: sys.intern('GOAWAY'),
        0x01: sys.intern('HEADERS'),
        0x06: sys.intern('PING'),
        0x02: sys.intern('PRIORITY'),
        0x05: sys.intern('PUSHPROMISE'),
        0x03: sys.intern('RESET'),
        0x04: sys.intern('SETTINGS'),
        0x08: sys.intern('WINDOWUPDATE')
    }

    def __init__(self, stream_id: int, frame_type: int, flags: Iterable[str] = (), parsed_flag_byte: int = 0, **kwargs: Any) -> None:
        #: The stream identifier for the stream this frame was received on.
        #: Set to 0 for frames sent on the connection (stream-id 0).
        self.stream_id = stream_id
        self.type = frame_type
        self.frame_type = self.frame_types.get(self.type)
        self.field = b''
        self.data = b''
        self.settings = {}
        self.origin = b''
        self.error_code = 0
        self.pad_length = 0
        self.last_stream_id = 0
        self.additional_data = 0
        self.depends_on = 0x0
        self.stream_weight = 0x0
        self.exclusive = False
        self.opaque_data = b''
        self.promised_stream_id = 0
        self.window_increment = 0
        self.flag_byte = 0x0
        self.defined_flags: List[Flag] = []

        #: The flags set for this frame.
        self.flags = Flags(self.defined_flags)

        #: The frame length, excluding the nine-byte header.
        self.body_len = 0

        for flag in flags:
            self.flags.add(flag)

        if self.type == 0xA:
            # ALTSVC
            self.origin = kwargs.get('origin', b'')
            self.field = kwargs.get('fields', b'')

        elif self.type == 0x09:
            # CONTINUATION

            self.defined_flags = [
                Flag('END_HEADERS', 0x04)
            ]

            self.data = kwargs.get('data')

        elif self.type == 0x0:
            # DATA

            self.defined_flags = [
                Flag('END_STREAM', 0x01),
                Flag('PADDED', 0x08),
            ]

            self.pad_length = kwargs.get('pad_length', 0)
            self.data = kwargs.get('data', b'')

        elif self.type == 0x07:
            # GOAWAY
            self.last_stream_id = kwargs.get('last_stream_id', 0)
            self.additional_data = kwargs.get('additional_data', b'')
            self.error_code = kwargs.get('error_code', 0)

        elif self.type == 0x01:
            # HEADERS
            self.defined_flags = [
                Flag('END_STREAM', 0x01),
                Flag('END_HEADERS', 0x04),
                Flag('PADDED', 0x08),
                Flag('PRIORITY', 0x20),
            ]

            self.data = kwargs.get('data', b'')
            self.pad_length = kwargs.get('pad_length', 0)
            self.depends_on = kwargs.get('depends_on', 0x0)
            self.stream_weight = kwargs.get('stream_weight', 0x0)
            self.exclusive = kwargs.get('exclusive', False)

        elif self.type == 0x06:
            # PING

            self.defined_flags = [
                Flag('ACK', 0x01)
            ]

            self.opaque_data = kwargs.get('opaque_data', b'')

        elif self.type == 0x02:
            # PRIORITY
            self.depends_on = kwargs.get('depends_on', 0x0)
            self.stream_weight = kwargs.get('stream_weight', 0x0)
            self.exclusive = kwargs.get('exclusive', False)

        elif self.type == 0x05:
            # PUSH PROMISE
            self.defined_flags = [
                Flag('END_HEADERS', 0x04),
                Flag('PADDED', 0x08)
            ]

            self.promised_stream_id = kwargs.get('promised_stream_id', 0)
            self.pad_length = kwargs.get('pad_length', 0)
            self.data = kwargs.get('data', b'')

        elif self.type == 0x03:
            # RESET
            self.error_code = kwargs.get('error_code', 0)

        elif self.type == 0x04:
            # SETTINGS
            self.defined_flags = [
                Flag('ACK', 0x01)
            ]

            self.settings = kwargs.get('settings', {})

        elif self.type == 0x08:
            # WINDOW UPDATE
            self.window_increment = kwargs.get('window_increment', 0)

        else:
            # EXTENSION
            self.flag_byte = kwargs.get('flag_byte', 0x0)

        for flag, flag_bit in self.defined_flags:
            if parsed_flag_byte & flag_bit:
                self.flags.add(flag)

    def __repr__(self) -> str:
        body_repr = self._body_repr(),
        return f"{type(self).__name__}(stream_id={self.stream_id}, flags={repr(self.flags)}): {body_repr}"

    def _body_repr(self) -> str:
        # More specific implementation may be provided by subclasses of Frame.
        # This fallback shows the serialized (and truncated) body content.
        return raw_data_repr(self.serialize())

    @property
    def flow_controlled_length(self) -> int:
        """
        The length of the frame that needs to be accounted for when considering
        flow control.
        """
        padding_len = 0
        if 'PADDED' in self.flags:
            # Account for extra 1-byte padding length field, which is still
            # present if possibly zero-valued.
            padding_len = self.pad_length + 1
        return len(self.data) + padding_len

    def parse_flags(self, flag_byte: int) -> Flags:

        for flag, flag_bit in self.defined_flags:
            if flag_byte & flag_bit:
                self.flags.add(flag)

        return self.flags

    def serialize(self) -> bytes:
        """
        Convert a frame into a bytestring, representing the serialized form of
        the frame.
        """

        body = b''
        flags = 0

        if self.type == 0xA:
            # ALTSVC
            origin_len = _STRUCT_H.pack(len(self.origin))    
            body = origin_len + self.origin + self.field

        elif self.type == 0x09:
            # CONTINUATION

            body = self.data

        elif self.type == 0x0:
            # DATA

            padding_data = b''
            if 'PADDED' in self.flags:  # type: ignore
                padding_data = _STRUCT_B.pack(self.pad_length)


            padding = b'\0' * self.pad_length
            body = padding_data + self.data + padding

        elif self.type == 0x07:
            # GOAWAY

            self.data = _STRUCT_LL.pack(
                self.last_stream_id & 0x7FFFFFFF,
                self.error_code
            )
            
            body = self.data + self.additional_data

        elif self.type == 0x01:
            # HEADERS

            padding_data = b''
            if 'PADDED' in self.flags:  # type: ignore
                padding_data = _STRUCT_B.pack(self.pad_length)

            padding = b'\0' * self.pad_length

            if 'PRIORITY' in self.flags:
                priority_data = _STRUCT_LB.pack(
                    self.depends_on + (0x80000000 if self.exclusive else 0),
                    self.stream_weight
                )
            else:
                priority_data = b''

            body = padding_data + priority_data + self.data + padding

        elif self.type == 0x06:
            # PING
            
            body = self.opaque_data
            body += b'\x00' * (8 - len(body))

        elif self.type == 0x02:
            # PRIORITY
            body = _STRUCT_LB.pack(
                self.depends_on + (0x80000000 if self.exclusive else 0),
                self.stream_weight
            )

        elif self.type == 0x05:
            # PUSH PROMISE

            padding_data = b''
            if 'PADDED' in self.flags:  # type: ignore
                padding_data = _STRUCT_B.pack(self.pad_length)

            padding = b'\0' * self.pad_length
            promise_data = _STRUCT_L.pack(self.promised_stream_id)

            body = padding_data + promise_data + self.data + padding

        elif self.type == 0x03:
            # RESET
            body = _STRUCT_L.pack(self.error_code)

        elif self.type == 0x04:
            # SETTINGS
            for setting, value in self.settings.items():
                body += _STRUCT_HL.pack(setting & 0xFF, value)

        elif self.type == 0x08:
            # WINDOW UPDATE
            body = _STRUCT_L.pack(self.window_increment & 0x7FFFFFFF)

        else:
            # EXTENSION
            flags = self.flag_byte

        self.body_len = len(body)


        for flag, flag_bit in self.defined_flags:
            if flag in self.flags:
                flags |= flag_bit

        header = _STRUCT_HBBBL.pack(
            (self.body_len >> 8) & 0xFFFF,  # Length spread over top 24 bits
            self.body_len & 0xFF,
            self.type,
            flags,
            self.stream_id & 0x7FFFFFFF  # Stream ID is 32 bits.
        )

        return header + body

    def get_events_and_frames(self, stream: Stream, connection):

        if self.type == 0x0:
            # DATA
            end_stream = 'END_STREAM' in self.flags
            flow_controlled_length = self.flow_controlled_length
            frame_data = self.data

            frames = []
            data_events = []
            connection._inbound_flow_control_window_manager.window_consumed(
                flow_controlled_length
            )

            try:
                
                stream.inbound.window_consumed(flow_controlled_length)
        
                event = DataReceived()
                event.stream_id = stream.stream_id

                data_events.append(event)

                if end_stream:
                    event = StreamEnded()
                    event.stream_id = stream.stream_id
                    data_events[0].stream_ended = event
                    data_events.append(event)

                data_events[0].data = frame_data
                data_events[0].flow_controlled_length = flow_controlled_length
                return frames, data_events

            except StreamClosedError as e:
                # This stream is either marked as CLOSED or already gone from our
                # internal state.
                
                conn_manager = connection._inbound_flow_control_window_manager
                conn_increment = conn_manager.process_bytes(
                    flow_controlled_length
                )

                if conn_increment:
                    f = Frame(0)
                    f.window_increment = conn_increment
                    frames.append(f)

                f = Frame(e.stream_id, 0x03)
                f.error_code = e.error_code
                frames.append(f)
        
                return frames, data_events + e._events

        elif self.type == 0x07:
            # GOAWAY
            self._data_to_send = b''

            new_event = ConnectionTerminated()
            new_event.error_code = h2.errors._error_code_from_int(self.error_code)
            new_event.last_stream_id = self.last_stream_id
            
            if self.additional_data:
                new_event.additional_data = self.additional_data

            return [], [new_event]

        elif self.type == 0x01:
            # HEADERS

            stream_events = []
            deferred_headers = DeferredHeaders(
                stream.encoder,
                self,
                connection._h2_state.config.header_encoding
            )

            stream_events.append(deferred_headers)

            if deferred_headers.end_stream:
                event = StreamEnded()
                event.stream_id = stream.stream_id
                
                stream_events[0].stream_ended = event
                stream_events.append(event)

            return [], stream_events

        elif self.type == 0x03:
            # RESET

            stream.closed_by = StreamClosedBy.RECV_RST_STREAM
            reset_event = StreamReset()
            reset_event.stream_id = stream.stream_id
            reset_event[0].error_code = h2.errors._error_code_from_int(self.error_code)

            return [], [reset_event]

        elif self.type == 0x04:
            # SETTINGS
            stream_events = []
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
                stream_events.append(ack_event)
                return [], stream_events

            # Add the new settings.
            connection.remote_settings.update(self.settings)
            stream_events.append(
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

            frames = Frame(0, 0x04)
            frames.flags.add('ACK')

            return [frames], stream_events

        elif self.type == 0x08:
            # WINDOW UPDATE
            stream_events = []
            frames = []
            increment = self.window_increment
            if self.stream_id:
                try:

                    
                    event = WindowUpdated()
                    event.stream_id = stream.stream_id

                    # If we encounter a problem with incrementing the flow control window,
                    # this should be treated as a *stream* error, not a *connection* error.
                    # That means we need to catch the error and forcibly close the stream.
                    event.delta = increment

                    try:
                        connection.outbound_flow_control_window = connection._guard_increment_window(
                            connection.outbound_flow_control_window,
                            increment
                        )
                    except StreamError:
                        # Ok, this is bad. We're going to need to perform a local
                        # reset.

                        event = StreamReset()
                        event.stream_id = stream.stream_id
                        event.error_code = ErrorCodes.FLOW_CONTROL_ERROR
                        event.remote_reset = False

                        stream.closed_by = ErrorCodes.FLOW_CONTROL_ERROR    
                        
                        rsf = Frame(stream.stream_id, 0x03)
                        rsf.error_code = ErrorCodes.FLOW_CONTROL_ERROR

                        frames = [rsf]

                    stream_events.append(event)
                except Exception:
                    return [], stream_events
            else:
                connection.outbound_flow_control_window = connection._guard_increment_window(
                    connection.outbound_flow_control_window,
                    increment
                )
                # FIXME: Should we split this into one event per active stream?
                window_updated_event = WindowUpdated()
                window_updated_event.stream_id = 0
                window_updated_event.delta = increment
                stream_events.append(window_updated_event)
                frames = []

            return frames, stream_events

        return None, None
