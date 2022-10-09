from asyncio import (
    Transport,
    DatagramTransport,
    Protocol,
    sleep
)


class Writer:
    __slots__ = (
        '_transport',
        '_protocol',
        '_reader',
        '_loop',
        '_complete_fut'
    )
    """Wraps a Transport.
    This exposes write(), writelines(), [can_]write_eof(),
    get_extra_info() and close().  It adds drain() which returns an
    optional Future on which you can wait for flow control.  It also
    adds a transport property which references the Transport
    directly.
    """

    def __init__(self, transport, protocol, reader, loop):
        self._transport: Transport | DatagramTransport = transport
        self._protocol: Protocol = protocol
        # drain() expects that the reader has an exception() method
        self._reader = reader
        self._loop = loop
        self._complete_fut = self._loop.create_future()
        self._complete_fut.set_result(None)

    def __repr__(self):
        info = [self.__class__.__name__, f'transport={self._transport!r}']
        if self._reader is not None:
            info.append(f'reader={self._reader!r}')
        return '<{}>'.format(' '.join(info))

    @property
    def transport(self):
        return self._transport

    def write(self, data):
        self._transport.write(data)

    def send(self, data):
        self._transport.sendto(data)

    def writelines(self, data):
        self._transport.writelines(data)

    def write_eof(self):
        return self._transport.write_eof()

    def can_write_eof(self):
        return self._transport.can_write_eof()

    def close(self):
        return self._transport.close()

    def is_closing(self):
        return self._transport.is_closing()

    async def wait_closed(self):
        await self._protocol._get_close_waiter(self)

    def get_extra_info(self, name, default=None):
        return self._transport.get_extra_info(name, default)

    async def drain(self):
        """Flush the write buffer.
        The intended use is to write
          w.write(data)
          await w.drain()
        """
        if self._reader is not None:
            exc = self._reader.exception()
            if exc is not None:
                raise exc
        if self._transport.is_closing():
            # Wait for protocol.connection_lost() call
            # Raise connection closing error if any,
            # ConnectionResetError otherwise
            # Yield to the event loop so connection_lost() may be
            # called.  Without this, _drain_helper() would return
            # immediately, and code that calls
            #     write(...); await drain()
            # in a loop would never call connection_lost(), so it
            # would not see an error when the socket is closed.
            await sleep(0)
        await self._protocol._drain_helper()

    async def start_tls(self, sslcontext, *, server_hostname=None,  ssl_handshake_timeout=None):

        server_side = self._protocol._client_connected_cb is not None
        protocol = self._protocol

        await self.drain()

        new_transport = await self._loop.start_tls(  # type: ignore
            self._transport, 
            protocol, 
            sslcontext,
            server_side=server_side, 
            server_hostname=server_hostname,
            ssl_handshake_timeout=ssl_handshake_timeout
        )
        
        self._transport = new_transport
        protocol._replace_writer(self)