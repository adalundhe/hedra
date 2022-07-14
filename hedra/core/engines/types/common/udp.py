from abc import ABCMeta, abstractmethod
from asyncio import DatagramProtocol
import asyncio
import socket
from asyncio.base_events import _run_until_complete_cb  # type: ignore[attr-defined]
from collections import deque
from types import TracebackType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Generic,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
    final,
    overload,
)


T_Attr = TypeVar("T_Attr")
T_Default = TypeVar("T_Default")
undefined = object()

T_Item = TypeVar("T_Item")
T_Stream = TypeVar("T_Stream")


class EndOfStream(Exception):
    """Raised when trying to read from a stream that has been closed from the other end."""

T = TypeVar("T")

class TypedAttributeProvider:
    """Base class for classes that wish to provide typed extra attributes."""

    @property
    def extra_attributes(self) -> Mapping[T_Attr, Callable[[], T_Attr]]:
        """
        A mapping of the extra attributes to callables that return the corresponding values.
        If the provider wraps another provider, the attributes from that wrapper should also be
        included in the returned mapping (but the wrapper may override the callables from the
        wrapped instance).
        """
        return {}

    @overload
    def extra(self, attribute: T_Attr) -> T_Attr:
        ...

    @overload
    def extra(self, attribute: T_Attr, default: T_Default) -> Union[T_Attr, T_Default]:
        ...

    @final
    def extra(self, attribute: Any, default: object = undefined) -> object:
        """
        extra(attribute, default=undefined)
        Return the value of the given typed extra attribute.
        :param attribute: the attribute (member of a :class:`~TypedAttributeSet`) to look for
        :param default: the value that should be returned if no value is found for the attribute
        :raises ~anyio.TypedAttributeLookupError: if the search failed and no default value was
            given
        """
        try:
            return self.extra_attributes[attribute]()
        except KeyError:
            if default is undefined:
                raise TypedAttributeLookupError("Attribute not found") from None
            else:
                return default



class AsyncResource(metaclass=ABCMeta):
    """
    Abstract base class for all closeable asynchronous resources.
    Works as an asynchronous context manager which returns the instance itself on enter, and calls
    :meth:`aclose` on exit.
    """

    async def __aenter__(self: T) -> T:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        await self.aclose()

    @abstractmethod
    async def aclose(self) -> None:
        """Close the resource."""


class UnreliableObjectReceiveStream(
    Generic[T_Item], AsyncResource, TypedAttributeProvider
):
    """
    An interface for receiving objects.
    This interface makes no guarantees that the received messages arrive in the order in which they
    were sent, or that no messages are missed.
    Asynchronously iterating over objects of this type will yield objects matching the given type
    parameter.
    """

    def __aiter__(self) -> "UnreliableObjectReceiveStream[T_Item]":
        return self

    async def __anext__(self) -> T_Item:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration

    @abstractmethod
    async def receive(self) -> T_Item:
        """
        Receive the next item.
        :raises ~anyio.ClosedResourceError: if the receive stream has been explicitly
            closed
        :raises ~anyio.EndOfStream: if this stream has been closed from the other end
        :raises ~anyio.BrokenResourceError: if this stream has been rendered unusable
            due to external causes
        """


class UnreliableObjectSendStream(
    Generic[T_Item], AsyncResource, TypedAttributeProvider
):
    """
    An interface for sending objects.
    This interface makes no guarantees that the messages sent will reach the recipient(s) in the
    same order in which they were sent, or at all.
    """

    @abstractmethod
    async def send(self, item: T_Item) -> None:
        """
        Send an item to the peer(s).
        :param item: the item to send
        :raises ~anyio.ClosedResourceError: if the send stream has been explicitly
            closed
        :raises ~anyio.BrokenResourceError: if this stream has been rendered unusable
            due to external causes
        """


class UnreliableObjectStream(
    UnreliableObjectReceiveStream[T_Item], UnreliableObjectSendStream[T_Item]
):
    """
    A bidirectional message stream which does not guarantee the order or reliability of message
    delivery.
    """


class BrokenResourceError(Exception):
    """
    Raised when trying to use a resource that has been rendered unusable due to external causes
    (e.g. a send stream whose peer has disconnected).
    """


class BusyResourceError(Exception):
    """Raised when two tasks are trying to read from or write to the same resource concurrently."""

    def __init__(self, action: str):
        super().__init__(f"Another task is already {action} this resource")


class ClosedResourceError(Exception):
    """Raised when trying to use a resource that has been closed."""

class TypedAttributeLookupError(LookupError):
    """
    Raised by :meth:`~anyio.TypedAttributeProvider.extra` when the given typed attribute is not
    found and no default value has been given.
    """


def typed_attribute() -> Any:
    """Return a unique object, used to mark typed attributes."""
    return object()


class TypedAttributeSet:
    """
    Superclass for typed attribute collections.
    Checks that every public attribute of every subclass has a type annotation.
    """

    def __init_subclass__(cls) -> None:
        annotations: Dict[str, Any] = getattr(cls, "__annotations__", {})
        for attrname in dir(cls):
            if not attrname.startswith("_") and attrname not in annotations:
                raise TypeError(
                    f"Attribute {attrname!r} is missing its type annotation"
                )

        super().__init_subclass__()



IPSockAddrType = Tuple[str, int]
SockAddrType = Union[IPSockAddrType, str]
IPSockAddrType = Tuple[str, int]
UDPPacketType = Tuple[bytes, IPSockAddrType]

class SocketAttribute(TypedAttributeSet):
    #: the address family of the underlying socket
    family: socket.AddressFamily = typed_attribute()
    #: the local socket address of the underlying socket
    local_address: SockAddrType = typed_attribute()
    #: for IP addresses, the local port the underlying socket is bound to
    local_port: int = typed_attribute()
    #: the underlying stdlib socket object
    raw_socket: socket.socket = typed_attribute()
    #: the remote address the underlying socket is connected to
    remote_address: SockAddrType = typed_attribute()
    #: for IP addresses, the remote port the underlying socket is connected to
    remote_port: int = typed_attribute()



class _SocketProvider(TypedAttributeProvider):
    @property
    def extra_attributes(self) -> Mapping[Any, Callable[[], Any]]:

        attributes: Dict[Any, Callable[[], Any]] = {
            SocketAttribute.family: lambda: self._raw_socket.family,
            SocketAttribute.local_address: lambda: convert_ipv6_sockaddr(
                self._raw_socket.getsockname()
            ),
            SocketAttribute.raw_socket: lambda: self._raw_socket,
        }
        try:
            peername: Optional[Tuple[str, int]] = convert_ipv6_sockaddr(
                self._raw_socket.getpeername()
            )
        except OSError:
            peername = None

        # Provide the remote address for connected sockets
        if peername is not None:
            attributes[SocketAttribute.remote_address] = lambda: peername

        # Provide local and remote ports for IP based sockets
        if self._raw_socket.family in (socket.AddressFamily.AF_INET, socket.AddressFamily.AF_INET6):
            attributes[
                SocketAttribute.local_port
            ] = lambda: self._raw_socket.getsockname()[1]
            if peername is not None:
                remote_port = peername[1]
                attributes[SocketAttribute.remote_port] = lambda: remote_port

        return attributes

    @property
    @abstractmethod
    def _raw_socket(self) -> socket.socket:
        pass


class AbstractConnectedUDPSocket(UnreliableObjectStream[bytes], _SocketProvider):
    """
    Represents an connected UDP socket.
    Supports all relevant extra attributes from :class:`~SocketAttribute`.
    """




class ResourceGuard:
    __slots__ = "action", "_guarded"

    def __init__(self, action: str):
        self.action = action
        self._guarded = False

    def __enter__(self) -> None:
        if self._guarded:
            raise BusyResourceError(self.action)

        self._guarded = True

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        self._guarded = False
        return None

async def checkpoint() -> None:
    await asyncio.sleep(0)


def convert_ipv6_sockaddr(
    sockaddr: Union[Tuple[str, int, int, int], Tuple[str, int]]
) -> Tuple[str, int]:
    """
    Convert a 4-tuple IPv6 socket address to a 2-tuple (address, port) format.
    If the scope ID is nonzero, it is added to the address, separated with ``%``.
    Otherwise the flow id and scope id are simply cut off from the tuple.
    Any other kinds of socket addresses are returned as-is.
    :param sockaddr: the result of :meth:`~socket.socket.getsockname`
    :return: the converted socket address
    """
    # This is more complicated than it should be because of MyPy
    if isinstance(sockaddr, tuple) and len(sockaddr) == 4:
        host, port, flowinfo, scope_id = cast(Tuple[str, int, int, int], sockaddr)
        if scope_id:
            # Add scope_id to the address
            return f"{host}%{scope_id}", port
        else:
            return host, port
    else:
        return cast(Tuple[str, int], sockaddr)



class UDPDatagramProtocol(asyncio.DatagramProtocol):
    read_queue: Deque[Tuple[bytes, IPSockAddrType]]
    read_event: asyncio.Event
    write_event: asyncio.Event
    exception: Optional[Exception] = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.read_queue = deque(maxlen=100)  # arbitrary value
        self.read_event = asyncio.Event()
        self.write_event = asyncio.Event()
        self.write_event.set()

    def connection_lost(self, exc: Optional[Exception]) -> None:
        self.read_event.set()
        self.write_event.set()

    def datagram_received(self, data: bytes, addr: IPSockAddrType) -> None:
        addr = convert_ipv6_sockaddr(addr)
        self.read_queue.append((data, addr))
        self.read_event.set()

    def error_received(self, exc: Exception) -> None:
        self.exception = exc

    def pause_writing(self) -> None:
        self.write_event.clear()

    def resume_writing(self) -> None:
        self.write_event.set()


class _RawSocketMixin:
    _receive_future: Optional[asyncio.Future] = None
    _send_future: Optional[asyncio.Future] = None
    _closing = False

    def __init__(self, raw_socket: socket.socket):
        self.__raw_socket = raw_socket
        self._receive_guard = ResourceGuard("reading from")
        self._send_guard = ResourceGuard("writing to")

    @property
    def _raw_socket(self) -> socket.socket:
        return self.__raw_socket

    def _wait_until_readable(self, loop: asyncio.AbstractEventLoop) -> asyncio.Future:
        def callback(f: object) -> None:
            del self._receive_future
            loop.remove_reader(self.__raw_socket)

        f = self._receive_future = asyncio.Future()
        loop.add_reader(self.__raw_socket, f.set_result, None)
        f.add_done_callback(callback)
        return f

    def _wait_until_writable(self, loop: asyncio.AbstractEventLoop) -> asyncio.Future:
        def callback(f: object) -> None:
            del self._send_future
            loop.remove_writer(self.__raw_socket)

        f = self._send_future = asyncio.Future()
        loop.add_writer(self.__raw_socket, f.set_result, None)
        f.add_done_callback(callback)
        return f

    async def aclose(self) -> None:
        if not self._closing:
            self._closing = True
            if self.__raw_socket.fileno() != -1:
                self.__raw_socket.close()

            if self._receive_future:
                self._receive_future.set_result(None)
            if self._send_future:
                self._send_future.set_result(None)



class AbstractUDPSocket(UnreliableObjectStream[UDPPacketType], _SocketProvider):
    """
    Represents an unconnected UDP socket.
    Supports all relevant extra attributes from :class:`~SocketAttribute`.
    """

    async def sendto(self, data: bytes, host: str, port: int) -> None:
        """Alias for :meth:`~.UnreliableObjectSendStream.send` ((data, (host, port)))."""
        return await self.send((data, (host, port)))




class UDPSocket(AbstractUDPSocket):
    def __init__(
        self, transport: asyncio.DatagramTransport, protocol: UDPDatagramProtocol
    ):
        self._transport = transport
        self._protocol = protocol
        self._receive_guard = ResourceGuard("reading from")
        self._send_guard = ResourceGuard("writing to")
        self._closed = False

    @property
    def _raw_socket(self) -> socket.socket:
        return self._transport.get_extra_info("socket")

    async def aclose(self) -> None:
        if not self._transport.is_closing():
            self._closed = True
            self._transport.close()

    async def receive(self) -> Tuple[bytes, IPSockAddrType]:
        with self._receive_guard:
            await checkpoint()
            # If the buffer is empty, ask for more data
            if not self._protocol.read_queue and not self._transport.is_closing():
                self._protocol.read_event.clear()
                await self._protocol.read_event.wait()

            try:
                return self._protocol.read_queue.popleft()
            except IndexError:
                if self._closed:
                    raise ClosedResourceError from None
                else:
                    raise BrokenResourceError from None

    async def send(self, item: UDPPacketType) -> None:
        with self._send_guard:
            await checkpoint()
            await self._protocol.write_event.wait()
            if self._closed:
                raise ClosedResourceError
            elif self._transport.is_closing():
                raise BrokenResourceError
            else:
                self._transport.sendto(*item)


class ConnectedUDPSocket(AbstractConnectedUDPSocket):
    def __init__(
        self, transport: asyncio.DatagramTransport, protocol: UDPDatagramProtocol
    ):
        self._transport = transport
        self._protocol = protocol
        self._receive_guard = ResourceGuard("reading from")
        self._send_guard = ResourceGuard("writing to")
        self._closed = False

    @property
    def _raw_socket(self) -> socket.socket:
        return self._transport.get_extra_info("socket")

    async def aclose(self) -> None:
        if not self._transport.is_closing():
            self._closed = True
            self._transport.close()

    async def receive(self) -> bytes:
        with self._receive_guard:
            await checkpoint()
            # If the buffer is empty, ask for more data
            if not self._protocol.read_queue and not self._transport.is_closing():
                self._protocol.read_event.clear()
                await self._protocol.read_event.wait()

            try:
                packet = self._protocol.read_queue.popleft()
            except IndexError:
                if self._closed:
                    raise ClosedResourceError from None
                else:
                    raise BrokenResourceError from None

            return packet[0]

    async def send(self, item: bytes) -> None:
        with self._send_guard:
            await checkpoint()
            await self._protocol.write_event.wait()
            if self._closed:
                raise ClosedResourceError
            elif self._transport.is_closing():
                raise BrokenResourceError
            else:
                self._transport.sendto(item)


# async def create_udp_socket(
#     family: socket.AddressFamily,
#     local_address: Optional[IPSockAddrType],
#     remote_address: Optional[IPSockAddrType],
#     reuse_port: bool,
# ) -> Union[UDPSocket, ConnectedUDPSocket]:
#     result = await asyncio.get_running_loop().create_datagram_endpoint(
#         DatagramProtocol,
#         local_addr=local_address,
#         remote_addr=remote_address,
#         family=family,
#         reuse_port=reuse_port,
#     )
#     transport = cast(asyncio.DatagramTransport, result[0])
#     protocol = result[1]
#     if protocol.exception:
#         transport.close()
#         raise protocol.exception

#     if not remote_address:
#         return UDPSocket(transport, protocol)
#     else:
#         return ConnectedUDPSocket(transport, protocol)
