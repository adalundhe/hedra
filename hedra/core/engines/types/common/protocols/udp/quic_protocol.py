from __future__ import annotations
import asyncio
import re
from enum import Enum, IntEnum
from collections import deque
from typing import (
    Deque, 
    Dict, 
    List, 
    Optional, 
    cast, 
    Set, 
    FrozenSet, 
    Callable,
    Tuple,
    Any,
    Union,
    Text,
)


def dummy_unit_encode(val: int):
    return b''


def dummy_stream_is_unidirectional(val: int):
    return False
    

try:

    import pylsqpack
    from aioquic.quic.connection import QuicConnection, NetworkAddress
    from aioquic.quic.events import (
        ConnectionTerminated,
        ConnectionIdRetired,
        ConnectionIdIssued,
        HandshakeCompleted,
        PingAcknowledged
    )
    from aioquic.h3.events import (
        DatagramReceived,
        DataReceived,
        H3Event,
        Headers,
        HeadersReceived,
        PushPromiseReceived,
        WebTransportStreamDataReceived,
    )
    from aioquic.buffer import (
        UINT_VAR_MAX_SIZE,
        Buffer, 
        BufferReadError, 
        encode_uint_var
    )

    from aioquic.quic.events import (
        DatagramFrameReceived, 
        QuicEvent, 
        StreamDataReceived
    )

    from aioquic.quic.connection import stream_is_unidirectional

except ImportError:
    pylsqpack = object
    aioquic = object
    QuicConnection = object
    ConnectionTerminated = object
    ConnectionIdRetired = object
    ConnectionIdIssued = object
    HandshakeCompleted = object
    PingAcknowledged = object
    DatagramReceived = object
    DataReceived = object
    H3Event = object
    Headers = Any
    HeadersReceived = object
    PushPromiseReceived = object
    WebTransportStreamDataReceived = object
    NetworkAddress = Any
    UINT_VAR_MAX_SIZE = 0 
    Buffer = object
    BufferReadError = object 
    encode_uint_var = dummy_unit_encode
    DatagramFrameReceived = object
    QuicEvent = object
    StreamDataReceived = object
    stream_is_unidirectional = dummy_stream_is_unidirectional


QuicConnectionIdHandler = Callable[[bytes], None]
QuicStreamHandler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], None]


USER_AGENT = "hedra/client"
RESERVED_SETTINGS = (0x0, 0x2, 0x3, 0x4, 0x5)
UPPERCASE = re.compile(b"[A-Z]")


class ErrorCode(IntEnum):
    H3_NO_ERROR = 0x100
    H3_GENERAL_PROTOCOL_ERROR = 0x101
    H3_INTERNAL_ERROR = 0x102
    H3_STREAM_CREATION_ERROR = 0x103
    H3_CLOSED_CRITICAL_STREAM = 0x104
    H3_FRAME_UNEXPECTED = 0x105
    H3_FRAME_ERROR = 0x106
    H3_EXCESSIVE_LOAD = 0x107
    H3_ID_ERROR = 0x108
    H3_SETTINGS_ERROR = 0x109
    H3_MISSING_SETTINGS = 0x10A
    H3_REQUEST_REJECTED = 0x10B
    H3_REQUEST_CANCELLED = 0x10C
    H3_REQUEST_INCOMPLETE = 0x10D
    H3_MESSAGE_ERROR = 0x10E
    H3_CONNECT_ERROR = 0x10F
    H3_VERSION_FALLBACK = 0x110
    QPACK_DECOMPRESSION_FAILED = 0x200
    QPACK_ENCODER_STREAM_ERROR = 0x201
    QPACK_DECODER_STREAM_ERROR = 0x202


class Setting(IntEnum):
    QPACK_MAX_TABLE_CAPACITY = 0x1
    MAX_FIELD_SECTION_SIZE = 0x6
    QPACK_BLOCKED_STREAMS = 0x7

    # https://datatracker.ietf.org/doc/html/rfc9220#section-5
    ENABLE_CONNECT_PROTOCOL = 0x8
    # https://datatracker.ietf.org/doc/html/draft-ietf-masque-h3-datagram-05#section-9.1
    H3_DATAGRAM = 0xFFD277
    # https://datatracker.ietf.org/doc/html/draft-ietf-webtrans-http2-02#section-10.1
    ENABLE_WEBTRANSPORT = 0x2B603742

    # Dummy setting to check it is correctly ignored by the peer.
    # https://datatracker.ietf.org/doc/html/rfc9114#section-7.2.4.1
    DUMMY = 0x21


def encode_frame(frame_type: int, frame_data: bytes) -> bytes:
    frame_length = len(frame_data)
    buf = Buffer(capacity=frame_length + 2 * UINT_VAR_MAX_SIZE)
    buf.push_uint_var(frame_type)
    buf.push_uint_var(frame_length)
    buf.push_bytes(frame_data)
    return buf.data


def validate_headers(
    headers: Headers,
    allowed_pseudo_headers: FrozenSet[bytes],
    required_pseudo_headers: FrozenSet[bytes],
) -> None:
    after_pseudo_headers = False
    authority: Optional[bytes] = None
    path: Optional[bytes] = None
    scheme: Optional[bytes] = None
    seen_pseudo_headers: Set[bytes] = set()
    for key, value in headers:
        if UPPERCASE.search(key):
            raise MessageError("Header %r contains uppercase letters" % key)

        if key.startswith(b":"):
            # pseudo-headers
            if after_pseudo_headers:
                raise MessageError(
                    "Pseudo-header %r is not allowed after regular headers" % key
                )
            if key not in allowed_pseudo_headers:
                raise MessageError("Pseudo-header %r is not valid" % key)
            if key in seen_pseudo_headers:
                raise MessageError("Pseudo-header %r is included twice" % key)
            seen_pseudo_headers.add(key)

            # store value
            if key == b":authority":
                authority = value
            elif key == b":path":
                path = value
            elif key == b":scheme":
                scheme = value
        else:
            # regular headers
            after_pseudo_headers = True

    # check required pseudo-headers are present
    missing = required_pseudo_headers.difference(seen_pseudo_headers)
    if missing:
        raise MessageError("Pseudo-headers %s are missing" % sorted(missing))

    if scheme in (b"http", b"https"):
        if not authority:
            raise MessageError("Pseudo-header b':authority' cannot be empty")
        if not path:
            raise MessageError("Pseudo-header b':path' cannot be empty")


class ProtocolError(Exception):
    """
    Base class for protocol errors.

    These errors are not exposed to the API user, they are handled
    in :meth:`H3Connection.handle_event`.
    """

    error_code = ErrorCode.H3_GENERAL_PROTOCOL_ERROR

    def __init__(self, reason_phrase: str = ""):
        self.reason_phrase = reason_phrase

class QpackDecompressionFailed(ProtocolError):
    error_code = ErrorCode.QPACK_DECOMPRESSION_FAILED


class QpackDecoderStreamError(ProtocolError):
    error_code = ErrorCode.QPACK_DECODER_STREAM_ERROR


class QpackEncoderStreamError(ProtocolError):
    error_code = ErrorCode.QPACK_ENCODER_STREAM_ERROR


class ClosedCriticalStream(ProtocolError):
    error_code = ErrorCode.H3_CLOSED_CRITICAL_STREAM


class FrameUnexpected(ProtocolError):
    error_code = ErrorCode.H3_FRAME_UNEXPECTED


class MessageError(ProtocolError):
    error_code = ErrorCode.H3_MESSAGE_ERROR


class MissingSettingsError(ProtocolError):
    error_code = ErrorCode.H3_MISSING_SETTINGS


class SettingsError(ProtocolError):
    error_code = ErrorCode.H3_SETTINGS_ERROR


class StreamCreationError(ProtocolError):
    error_code = ErrorCode.H3_STREAM_CREATION_ERROR

class FrameType(IntEnum):
    DATA = 0x0
    HEADERS = 0x1
    PRIORITY = 0x2
    CANCEL_PUSH = 0x3
    SETTINGS = 0x4
    PUSH_PROMISE = 0x5
    GOAWAY = 0x7
    MAX_PUSH_ID = 0xD
    DUPLICATE_PUSH = 0xE
    WEBTRANSPORT_STREAM = 0x41

class StreamType(IntEnum):
    CONTROL = 0
    PUSH = 1
    QPACK_ENCODER = 2
    QPACK_DECODER = 3
    WEBTRANSPORT = 0x54

class HeadersState(Enum):
    INITIAL = 0
    AFTER_HEADERS = 1
    AFTER_TRAILERS = 2

class H3Stream:

    __slots__ =(
        'blocked',
        'blocked_frame_size',
        'buffer',
        'ended',
        'frame_size',
        'frame_type',
        'headers_recv_state',
        'headers_send_state',
        'push_id',
        'session_id',
        'stream_id',
        'stream_type'
    )

    def __init__(self, stream_id: int) -> None:
        self.blocked = False
        self.blocked_frame_size: Optional[int] = None
        self.buffer = b""
        self.ended = False
        self.frame_size: Optional[int] = None
        self.frame_type: Optional[int] = None
        self.headers_recv_state: HeadersState = HeadersState.INITIAL
        self.headers_send_state: HeadersState = HeadersState.INITIAL
        self.push_id: Optional[int] = None
        self.session_id: Optional[int] = None
        self.stream_id = stream_id
        self.stream_type: Optional[int] = None


class ResponseFrameCollection:

    __slots__ = (
        'headers_frame',
        'body'
    )

    def __init__(self) -> None:
        self.headers_frame: HeadersReceived = None
        self.body = bytearray()


class QuicStreamAdapter(asyncio.Transport):

    __slots__ = (
        'protocol',
        'stream_id'
    )

    def __init__(self, protocol: QuicProtocol, stream_id: int):
        self.protocol = protocol
        self.stream_id = stream_id

    def can_write_eof(self) -> bool:
        return True

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """
        Get information about the underlying QUIC stream.
        """
        if name == "stream_id":
            return self.stream_id

    def write(self, data):
        self.protocol._quic.send_stream_data(self.stream_id, data)
        self.protocol._transmit_soon()

    def write_eof(self):
        self.protocol._quic.send_stream_data(self.stream_id, b"", end_stream=True)
        self.protocol._transmit_soon()


class QuicProtocol(asyncio.DatagramProtocol):

    __slots__ = (
        '_loop',
        '_request_waiter',
        '_closed',
        '_connected',
        '_connected_waiter',
        '_ping_waiters',
        '_quic',
        '_stream_readers',
        '_timer',
        '_timer_at',
        '_transmit_task',
        '_transport',
        '_connection_id_issued_handler',
        '_connection_id_retired_handler',
        '_connection_terminated_handler',
        '_stream_handler',
        'pushes',
        '_request_events',
        '_request_waiter',
        '_stream',
        '_is_done',
        '_max_table_capacity',
        '_blocked_streams',
        '_decoder',
        '_decoder_bytes_received',
        '_decoder_bytes_sent',
        '_encoder',
        '_encoder_bytes_received',
        '_encoder_bytes_sent',
        '_settings_received',
        '_stream',
        '_max_push_id',
        '_next_push_id',
        '_local_control_stream_id',
        '_local_decoder_stream_id',
        '_local_encoder_stream_id',
        '_peer_control_stream_id',
        '_peer_decoder_stream_id',
        '_peer_encoder_stream_id',
        '_received_settings',
        '_sent_settings',
        'responses',
        '_is_closed'
    )

    def __init__(
        self, quic: QuicConnection, 
        stream_handler: Optional[QuicStreamHandler] = None,
        loop: asyncio.AbstractEventLoop = None
    ):
        
        self._loop = loop

        self._request_waiter: Dict[int, asyncio.Future[Deque[H3Event]]] = {}
        self._closed = asyncio.Event()
        self._connected = False
        self._connected_waiter: Optional[asyncio.Future[None]] = None
        self._ping_waiters: Dict[int, asyncio.Future[None]] = {}
        self._quic = quic
        self._stream_readers: Dict[int, asyncio.StreamReader] = {}
        self._timer: Optional[asyncio.TimerHandle] = None
        self._timer_at: Optional[float] = None
        self._transmit_task: Optional[asyncio.Handle] = None
        self._transport: Optional[asyncio.DatagramTransport] = None

        # callbacks
        self._connection_id_issued_handler: QuicConnectionIdHandler = lambda c: None
        self._connection_id_retired_handler: QuicConnectionIdHandler = lambda c: None
        self._connection_terminated_handler: Callable[[], None] = lambda: None
        if stream_handler is not None:
            self._stream_handler = stream_handler
        else:
            self._stream_handler = lambda r, w: None

        self.pushes: Dict[int, Deque[H3Event]] = {}
        self._request_events: Dict[int, Deque[H3Event]] = {}
        self._request_waiter: Dict[int, asyncio.Future[Deque[H3Event]]] = {}
        self._stream: Dict[int, H3Stream] = {}
        self._is_done = False
        self._max_table_capacity = 4096
        self._blocked_streams = 128

        self._decoder = pylsqpack.Decoder(
            self._max_table_capacity, self._blocked_streams
        )
        self._decoder_bytes_received = 0
        self._decoder_bytes_sent = 0
        self._encoder = pylsqpack.Encoder()
        self._encoder_bytes_received = 0
        self._encoder_bytes_sent = 0
        self._settings_received = False
        self._stream: Dict[int, H3Stream] = {}

        self._max_push_id: Optional[int] = None
        self._next_push_id: int = 0

        self._local_control_stream_id: Optional[int] = None
        self._local_decoder_stream_id: Optional[int] = None
        self._local_encoder_stream_id: Optional[int] = None

        self._peer_control_stream_id: Optional[int] = None
        self._peer_decoder_stream_id: Optional[int] = None
        self._peer_encoder_stream_id: Optional[int] = None
        self._received_settings: Optional[Dict[int, int]] = None
        self._sent_settings: Optional[Dict[int, int]] = None
        self.responses: Dict[int, ResponseFrameCollection] = {} 
        self._is_closed = False
        

    def init_connection(self) -> None:
        # send our settings
        self._local_control_stream_id = self._create_uni_stream(StreamType.CONTROL)
        self._sent_settings = {
            Setting.QPACK_MAX_TABLE_CAPACITY: self._max_table_capacity,
            Setting.QPACK_BLOCKED_STREAMS: self._blocked_streams,
            Setting.ENABLE_CONNECT_PROTOCOL: 1,
            Setting.DUMMY: 1,
        }

        buf = Buffer(capacity=1024)

        for setting, value in self._sent_settings.items():
            buf.push_uint_var(setting)
            buf.push_uint_var(value)

        self._quic.send_stream_data(
            self._local_control_stream_id,
            encode_frame(FrameType.SETTINGS, buf.data),
        )
        if self._max_push_id is not None:
            self._quic.send_stream_data(
                self._local_control_stream_id,
                encode_frame(FrameType.MAX_PUSH_ID, encode_uint_var(self._max_push_id)),
            )

        # create encoder and decoder streams
        self._local_encoder_stream_id = self._create_uni_stream(
            StreamType.QPACK_ENCODER
        )
        self._local_decoder_stream_id = self._create_uni_stream(
            StreamType.QPACK_DECODER
        )

    def _create_uni_stream(
        self, stream_type: int, push_id: Optional[int] = None
    ) -> int:
        """
        Create an unidirectional stream of the given type.
        """
        stream_id = self._quic.get_next_available_stream_id(is_unidirectional=True)
        self._quic.send_stream_data(stream_id, encode_uint_var(stream_type))
        return stream_id

    def change_connection_id(self) -> None:
        """
        Change the connection ID used to communicate with the peer.

        The previous connection ID will be retired.
        """
        self._quic.change_connection_id()
        self.transmit()

    def close(self) -> None:
        """
        Close the connection.
        """
        self._is_closed = True
        self._quic.close()
        self.transmit()

    def connect(self, addr: NetworkAddress) -> None:
        """
        Initiate the TLS handshake.

        This method can only be called for clients and a single time.
        """
        self._quic.connect(addr, now=self._loop.time())
        self.transmit()

    
    def _get_or_create_stream(self, stream_id: int) -> H3Stream:
        if stream_id not in self._stream:
            self._stream[stream_id] = H3Stream(stream_id)
        return self._stream[stream_id]
    

    async def create_stream(
        self, is_unidirectional: bool = False
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """
        Create a QUIC stream and return a pair of (reader, writer) objects.

        The returned reader and writer objects are instances of :class:`asyncio.StreamReader`
        and :class:`asyncio.StreamWriter` classes.
        """
        stream_id = self._quic.get_next_available_stream_id(
            is_unidirectional=is_unidirectional
        )
        return self._create_stream(stream_id)

    def request_key_update(self) -> None:
        """
        Request an update of the encryption keys.
        """
        self._quic.request_key_update()
        self.transmit()

    async def ping(self) -> None:
        """
        Ping the peer and wait for the response.
        """
        waiter = self._loop.create_future()
        uid = id(waiter)
        self._ping_waiters[uid] = waiter
        self._quic.send_ping(uid)
        self.transmit()
        await asyncio.shield(waiter)

    def transmit(self) -> None:
        """
        Send pending datagrams to the peer and arm the timer if needed.
        """
        self._transmit_task = None

        # send datagrams
        for data, addr in self._quic.datagrams_to_send(now=self._loop.time()):
            self._transport.sendto(data, addr)

        # re-arm timer
        timer_at = self._quic.get_timer()
        if self._timer is not None and self._timer_at != timer_at:
            self._timer.cancel()
            self._timer = None
        if self._timer is None and timer_at is not None:
            self._timer = self._loop.call_at(timer_at, self._handle_timer)
        self._timer_at = timer_at

    async def wait_closed(self) -> None:
        """
        Wait for the connection to be closed.
        """
        await self._closed.wait()

    async def wait_connected(self) -> None:
        """
        Wait for the TLS handshake to complete.
        """
        assert self._connected_waiter is None, "already awaiting connected"
        if not self._connected:
            self._connected_waiter = self._loop.create_future()
            await self._connected_waiter

    # asyncio.Transport

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast(asyncio.DatagramTransport, transport)

    def datagram_received(self, data: Union[bytes, Text], addr: NetworkAddress) -> None:
        self._quic.receive_datagram(cast(bytes, data), addr, now=self._loop.time())
        self._process_events()
        self.transmit()

    # overridable
    def quic_event_received(self, event: QuicEvent) -> None:
        #  pass event to the HTTP layer'
        events = []
        if not self._is_done:
            try:
                if isinstance(event, StreamDataReceived):
                    stream_id = event.stream_id
                    stream = self._get_or_create_stream(stream_id)
                    if stream_is_unidirectional(stream_id):

                        http_events: List[H3Event] = []

                        stream.buffer += event.data
                        if event.end_stream:
                            stream.ended = True

                        buf = Buffer(data=stream.buffer)
                        consumed = 0
                        unblocked_streams: Set[int] = set()

                        while (
                            stream.stream_type
                            in (StreamType.PUSH, StreamType.CONTROL, StreamType.WEBTRANSPORT)
                            or not buf.eof()
                        ):
                            # fetch stream type for unidirectional streams
                            if stream.stream_type is None:
                                try:
                                    stream.stream_type = buf.pull_uint_var()
                                except BufferReadError:
                                    break
                                consumed = buf.tell()

                                # check unicity
                                if stream.stream_type == StreamType.CONTROL:
                                    if self._peer_control_stream_id is not None:
                                        raise StreamCreationError("Only one control stream is allowed")
                                    self._peer_control_stream_id = stream.stream_id
                                elif stream.stream_type == StreamType.QPACK_DECODER:
                                    if self._peer_decoder_stream_id is not None:
                                        raise StreamCreationError(
                                            "Only one QPACK decoder stream is allowed"
                                        )
                                    self._peer_decoder_stream_id = stream.stream_id
                                elif stream.stream_type == StreamType.QPACK_ENCODER:
                                    if self._peer_encoder_stream_id is not None:
                                        raise StreamCreationError(
                                            "Only one QPACK encoder stream is allowed"
                                        )
                                    self._peer_encoder_stream_id = stream.stream_id



                            if stream.stream_type == StreamType.CONTROL:
                                if event.end_stream:
                                    raise ClosedCriticalStream("Closing control stream is not allowed")

                                # fetch next frame
                                try:
                                    frame_type = buf.pull_uint_var()
                                    frame_length = buf.pull_uint_var()
                                    frame_data = buf.pull_bytes(frame_length)
                                except BufferReadError:
                                    break
                                consumed = buf.tell()

                                if frame_type != FrameType.SETTINGS and not self._settings_received:
                                    raise MissingSettingsError

                                if frame_type == FrameType.SETTINGS:
                                    if self._settings_received:
                                        raise FrameUnexpected("SETTINGS have already been received")
                            
                                    buf = Buffer(data=frame_data)
                                    settings: Dict[int, int] = {}
                                    while not buf.eof():
                                        setting = buf.pull_uint_var()
                                        value = buf.pull_uint_var()
                                        if setting in RESERVED_SETTINGS:
                                            raise SettingsError("Setting identifier 0x%x is reserved" % setting)
                                        if setting in settings:
                                            raise SettingsError("Setting identifier 0x%x is included twice" % setting)
                                        settings[setting] = value   

                                    for setting in [
                                        Setting.ENABLE_CONNECT_PROTOCOL,
                                        Setting.ENABLE_WEBTRANSPORT,
                                        Setting.H3_DATAGRAM,
                                    ]:
                                        if setting in settings and settings[setting] not in (0, 1):
                                            raise SettingsError(f"{setting.name} setting must be 0 or 1")

                                    if (
                                        settings.get(Setting.H3_DATAGRAM) == 1
                                        and self._quic._remote_max_datagram_frame_size is None
                                    ):
                                        raise SettingsError(
                                            "H3_DATAGRAM requires max_datagram_frame_size transport parameter"
                                        )

                                    if (
                                        settings.get(Setting.ENABLE_WEBTRANSPORT) == 1
                                        and settings.get(Setting.H3_DATAGRAM) != 1
                                    ):
                                        raise SettingsError("ENABLE_WEBTRANSPORT requires H3_DATAGRAM")

                                    self._received_settings = settings
                                    encoder = self._encoder.apply_settings(
                                        max_table_capacity=settings.get(Setting.QPACK_MAX_TABLE_CAPACITY, 0),
                                        blocked_streams=settings.get(Setting.QPACK_BLOCKED_STREAMS, 0),
                                    )
                                    self._quic.send_stream_data(self._local_encoder_stream_id, encoder)
                                    self._settings_received = True

                                elif frame_type in (
                                    FrameType.DATA,
                                    FrameType.HEADERS,
                                    FrameType.PUSH_PROMISE,
                                    FrameType.DUPLICATE_PUSH,
                                ):
                                    raise FrameUnexpected("Invalid frame type on control stream")

                            elif stream.stream_type == StreamType.PUSH:
                                # fetch push id
                                if stream.push_id is None:
                                    try:
                                        stream.push_id = buf.pull_uint_var()
                                    except BufferReadError:
                                        break
                                    consumed = buf.tell()

                                # remove processed data from buffer
                                stream.buffer = stream.buffer[consumed:]

                                return self._receive_request_or_push_data(stream, b"", event.end_stream)
                            elif stream.stream_type == StreamType.WEBTRANSPORT:
                                # fetch session id
                                if stream.session_id is None:
                                    try:
                                        stream.session_id = buf.pull_uint_var()
                                    except BufferReadError:
                                        break
                                    consumed = buf.tell()

                                frame_data = stream.buffer[consumed:]
                                stream.buffer = b""

                                if frame_data or event.end_stream:
                                    http_events.append(
                                        WebTransportStreamDataReceived(
                                            data=frame_data,
                                            session_id=stream.session_id,
                                            stream_ended=stream.ended,
                                            stream_id=stream.stream_id,
                                        )
                                    )
                                return http_events
                            elif stream.stream_type == StreamType.QPACK_DECODER:
                                # feed unframed data to decoder
                                data = buf.pull_bytes(buf.capacity - buf.tell())
                                consumed = buf.tell()
                                try:
                                    self._encoder.feed_decoder(data)
                                except pylsqpack.DecoderStreamError as exc:
                                    raise QpackDecoderStreamError() from exc
                                self._decoder_bytes_received += len(data)
                            elif stream.stream_type == StreamType.QPACK_ENCODER:
                                # feed unframed data to encoder
                                data = buf.pull_bytes(buf.capacity - buf.tell())
                                consumed = buf.tell()
                                try:
                                    unblocked_streams.update(self._decoder.feed_encoder(data))
                                except pylsqpack.EncoderStreamError as exc:
                                    raise QpackEncoderStreamError() from exc
                                self._encoder_bytes_received += len(data)
                            else:
                                # unknown stream type, discard data
                                buf.seek(buf.capacity)
                                consumed = buf.tell()

                        # remove processed data from buffer
                        stream.buffer = stream.buffer[consumed:]

                        # process unblocked streams
                        for stream_id in unblocked_streams:
                            stream = self._stream[stream_id]

                            # resume headers
                            http_events.extend(
                                self._handle_request_or_push_frame(
                                    frame_type=FrameType.HEADERS,
                                    frame_data=None,
                                    stream=stream,
                                    stream_ended=stream.ended and not stream.buffer,
                                )
                            )
                            stream.blocked = False
                            stream.blocked_frame_size = None

                            # resume processing
                            if stream.buffer:
                                http_events.extend(
                                    self._receive_request_or_push_data(stream, b"", stream.ended)
                                )

                        events = http_events

                    else:
                        events = self._receive_request_or_push_data(
                            stream, event.data, event.end_stream
                        )
                elif isinstance(event, DatagramFrameReceived):
                    buf = Buffer(data=event.data)
                    try:
                        flow_id = buf.pull_uint_var()
                    except BufferReadError:
                        raise ProtocolError("Could not parse flow ID")
                    
                    events = [DatagramReceived(
                        data=event.data[buf.tell() :], 
                        flow_id=flow_id
                    )]

            except ProtocolError as exc:
                self._is_done = True
                self._quic.close(
                    error_code=exc.error_code, reason_phrase=exc.reason_phrase
                )

        for http_event in events:

            if isinstance(http_event, HeadersReceived):

                if self.responses.get(http_event.stream_id) is None:
                    self.responses[http_event.stream_id] = ResponseFrameCollection()

                self.responses[http_event.stream_id].headers_frame = http_event

                if http_event.push_id in self.pushes:
                    # push
                    self.pushes[http_event.push_id].append(http_event)


            elif isinstance(http_event, DataReceived):

                if self.responses.get(http_event.stream_id) is None:
                    self.responses[http_event.stream_id] = ResponseFrameCollection()

                self.responses[http_event.stream_id].body.extend(http_event.data)
                request_waiter = self._request_waiter.get(http_event.stream_id)

                if http_event.stream_ended and request_waiter is None:
                    raise Exception('Err. - Stream failed')
                
                elif http_event.stream_ended and request_waiter.done() is False:
                    request_waiter.set_result(self.responses.pop(http_event.stream_id))
                
                if http_event.push_id in self.pushes:
                    # push
                    self.pushes[http_event.push_id].append(http_event)

            elif isinstance(event, PushPromiseReceived):
                self.pushes[event.push_id] = deque()
                self.pushes[event.push_id].append(event)
    
    def _receive_request_or_push_data(
        self, stream: H3Stream, data: bytes, stream_ended: bool
    ) -> List[H3Event]:
        """
        Handle data received on a request or push stream.
        """
        http_events: List[H3Event] = []

        stream.buffer += data
        if stream_ended:
            stream.ended = True
        if stream.blocked:
            return http_events

        # shortcut for WEBTRANSPORT_STREAM frame fragments
        if (
            stream.frame_type == FrameType.WEBTRANSPORT_STREAM
            and stream.session_id is not None
        ):
            http_events.append(
                WebTransportStreamDataReceived(
                    data=stream.buffer,
                    session_id=stream.session_id,
                    stream_id=stream.stream_id,
                    stream_ended=stream_ended,
                )
            )
            stream.buffer = b""
            return http_events

        # shortcut for DATA frame fragments
        if (
            stream.frame_type == FrameType.DATA
            and stream.frame_size is not None
            and len(stream.buffer) < stream.frame_size
        ):
            http_events.append(
                DataReceived(
                    data=stream.buffer,
                    push_id=stream.push_id,
                    stream_id=stream.stream_id,
                    stream_ended=False,
                )
            )
            stream.frame_size -= len(stream.buffer)
            stream.buffer = b""
            return http_events

        # handle lone FIN
        if stream_ended and not stream.buffer:
            http_events.append(
                DataReceived(
                    data=b"",
                    push_id=stream.push_id,
                    stream_id=stream.stream_id,
                    stream_ended=True,
                )
            )
            return http_events

        buf = Buffer(data=stream.buffer)
        consumed = 0

        while not buf.eof():
            # fetch next frame header
            if stream.frame_size is None:
                try:
                    stream.frame_type = buf.pull_uint_var()
                    stream.frame_size = buf.pull_uint_var()
                except BufferReadError:
                    break
                consumed = buf.tell()

                # WEBTRANSPORT_STREAM frames last until the end of the stream
                if stream.frame_type == FrameType.WEBTRANSPORT_STREAM:
                    stream.session_id = stream.frame_size
                    stream.frame_size = None

                    frame_data = stream.buffer[consumed:]
                    stream.buffer = b""

                    if frame_data or stream_ended:
                        http_events.append(
                            WebTransportStreamDataReceived(
                                data=frame_data,
                                session_id=stream.session_id,
                                stream_id=stream.stream_id,
                                stream_ended=stream_ended,
                            )
                        )
                    return http_events

            # check how much data is available
            chunk_size = min(stream.frame_size, buf.capacity - consumed)
            if stream.frame_type != FrameType.DATA and chunk_size < stream.frame_size:
                break

            # read available data
            frame_data = buf.pull_bytes(chunk_size)
            frame_type = stream.frame_type
            consumed = buf.tell()

            # detect end of frame
            stream.frame_size -= chunk_size
            if not stream.frame_size:
                stream.frame_size = None
                stream.frame_type = None

            try:
                http_events.extend(
                    self._handle_request_or_push_frame(
                        frame_type=frame_type,
                        frame_data=frame_data,
                        stream=stream,
                        stream_ended=stream.ended and buf.eof(),
                    )
                )
            except pylsqpack.StreamBlocked:
                stream.blocked = True
                stream.blocked_frame_size = len(frame_data)
                break

        # remove processed data from buffer
        stream.buffer = stream.buffer[consumed:]

        return http_events

    def _create_stream(
        self, stream_id: int
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        adapter = QuicStreamAdapter(self, stream_id)
        reader = asyncio.StreamReader()
        writer = asyncio.StreamWriter(adapter, None, reader, self._loop)
        self._stream_readers[stream_id] = reader
        return reader, writer

    def _handle_timer(self) -> None:
        now = max(self._timer_at, self._loop.time())
        self._timer = None
        self._timer_at = None
        self._quic.handle_timer(now=now)
        self._process_events()
        self.transmit()

    def _process_events(self) -> None:
        event = self._quic.next_event()
        while event is not None:
            if isinstance(event, ConnectionIdIssued):
                self._connection_id_issued_handler(event.connection_id)
            elif isinstance(event, ConnectionIdRetired):
                self._connection_id_retired_handler(event.connection_id)
            elif isinstance(event, ConnectionTerminated):
                self._connection_terminated_handler()

                # abort connection waiter
                if self._connected_waiter is not None:
                    waiter = self._connected_waiter
                    self._connected_waiter = None
                    waiter.set_exception(ConnectionError)

                # abort ping waiters
                for waiter in self._ping_waiters.values():
                    waiter.set_exception(ConnectionError)
                self._ping_waiters.clear()

                self._closed.set()
            elif isinstance(event, HandshakeCompleted):
                if self._connected_waiter is not None:
                    waiter = self._connected_waiter
                    self._connected = True
                    self._connected_waiter = None
                    waiter.set_result(None)
            elif isinstance(event, PingAcknowledged):
                waiter = self._ping_waiters.pop(event.uid, None)
                if waiter is not None:
                    waiter.set_result(None)
            self.quic_event_received(event)
            event = self._quic.next_event()

    def _transmit_soon(self) -> None:
        if self._transmit_task is None:
            self._transmit_task = self._loop.call_soon(self.transmit)

    def _handle_request_or_push_frame(
        self,
        frame_type: int,
        frame_data: Optional[bytes],
        stream: H3Stream,
        stream_ended: bool,
    ) -> List[H3Event]:
        """
        Handle a frame received on a request or push stream.
        """
        http_events: List[H3Event] = []

        if frame_type == FrameType.DATA:
            # check DATA frame is allowed
            if stream.headers_recv_state != HeadersState.AFTER_HEADERS:
                raise FrameUnexpected("DATA frame is not allowed in this state")

            if stream_ended or frame_data:
                http_events.append(
                    DataReceived(
                        data=frame_data,
                        push_id=stream.push_id,
                        stream_ended=stream_ended,
                        stream_id=stream.stream_id,
                    )
                )
        elif frame_type == FrameType.HEADERS:
            # check HEADERS frame is allowed
            if stream.headers_recv_state == HeadersState.AFTER_TRAILERS:
                raise FrameUnexpected("HEADERS frame is not allowed in this state")

            # try to decode HEADERS, may raise pylsqpack.StreamBlocked
            headers = self._decode_headers(stream.stream_id, frame_data)

            # validate headers
            if stream.headers_recv_state == HeadersState.INITIAL:
                validate_headers(
                    headers,
                    allowed_pseudo_headers=frozenset((b":status",)),
                    required_pseudo_headers=frozenset((b":status",)),
                )

            else:
                validate_headers(
                    headers,
                    allowed_pseudo_headers=frozenset(),
                    required_pseudo_headers=frozenset(),
                )


            # update state and emit headers
            if stream.headers_recv_state == HeadersState.INITIAL:
                stream.headers_recv_state = HeadersState.AFTER_HEADERS
            else:
                stream.headers_recv_state = HeadersState.AFTER_TRAILERS
            http_events.append(
                HeadersReceived(
                    headers=headers,
                    push_id=stream.push_id,
                    stream_id=stream.stream_id,
                    stream_ended=stream_ended,
                )
            )
        elif frame_type == FrameType.PUSH_PROMISE and stream.push_id is None:
            
            frame_buf = Buffer(data=frame_data)
            push_id = frame_buf.pull_uint_var()
            headers = self._decode_headers(
                stream.stream_id, frame_data[frame_buf.tell() :]
            )

            # validate headers
            validate_headers(
                headers,
                allowed_pseudo_headers=frozenset(
                    (b":method", b":scheme", b":authority", b":path")
                ),
                required_pseudo_headers=frozenset(
                    (b":method", b":scheme", b":authority", b":path")
                ),
            )

            # emit event
            http_events.append(
                PushPromiseReceived(
                    headers=headers, push_id=push_id, stream_id=stream.stream_id
                )
            )
        elif frame_type in (
            FrameType.PRIORITY,
            FrameType.CANCEL_PUSH,
            FrameType.SETTINGS,
            FrameType.PUSH_PROMISE,
            FrameType.GOAWAY,
            FrameType.MAX_PUSH_ID,
            FrameType.DUPLICATE_PUSH,
        ):
            raise FrameUnexpected(
                "Invalid frame type on request stream"
                if stream.push_id is None
                else "Invalid frame type on push stream"
            )

        return http_events
    
    def _decode_headers(self, stream_id: int, frame_data: Optional[bytes]) -> Headers:
        """
        Decode a HEADERS block and send decoder updates on the decoder stream.

        This is called with frame_data=None when a stream becomes unblocked.
        """
        try:
            if frame_data is None:
                decoder, headers = self._decoder.resume_header(stream_id)
            else:
                decoder, headers = self._decoder.feed_header(stream_id, frame_data)
            self._decoder_bytes_sent += len(decoder)
            self._quic.send_stream_data(self._local_decoder_stream_id, decoder)
        except pylsqpack.DecompressionFailed as exc:
            raise QpackDecompressionFailed() from exc

        return headers
