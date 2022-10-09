import asyncio
from weakref import ref
from hedra.core.engines.types.common.protocols.shared.reader import Reader
from .protocol import TCPProtocol


class TLSProtocol(TCPProtocol):

    def upgrade_reader(self, reader: Reader):

        if self._stream_reader:
            self._stream_reader.set_exception(Exception('upgraded connection to TLS, this reader is obsolete now.'))

        self._stream_reader_wr = ref(reader)
        self._source_traceback = reader._source_traceback