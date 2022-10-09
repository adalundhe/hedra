from collections import deque
from typing import Dict, List, Tuple, Union
from hedra.core.engines.types.common.decoder import Decoder
from hedra.core.engines.types.common.encoder import Encoder
from .base_event import BaseEvent


def is_informational_response(headers: Tuple[bytes, bytes]):
    """
    Searches a header block for a :status header to confirm that a given
    collection of headers are an informational response. Assumes the header
    block is well formed: that is, that the HTTP/2 special headers are first
    in the block, and so that it can stop looking when it finds the first
    header field whose name does not begin with a colon.

    :param headers: The HTTP/2 header block.
    :returns: A boolean indicating if this is an informational response.
    """
    for n, v in headers:
        sigil = b':'
        status = b':status'
        informational_start = b'1'

        # If we find a non-special header, we're done here: stop looping.
        if not n.startswith(sigil):
            return False

        # This isn't the status header, bail.
        if n != status:
            continue

        # If the first digit is a 1, we've got informational headers.
        return v.startswith(informational_start)


# Parsing headers mid load-test is *expensive* so we want to defer
# this work until later.
class DeferredHeaders(BaseEvent):
    event_type='DEFERRED_HEADERS'

    __slots__ = (
        'stream_id',
        'hpack_table',
        'raw_headers',
        'stream_ended',
        'end_stream',
        'priority',
        'encoding',
        'priority_updated'
    )

    def __init__(self, encoder: Encoder, frame, encoding: Union[str, None]) -> None:
        super().__init__()
        self.stream_id = frame.stream_id
        self.hpack_table = encoder.header_table
        self.raw_headers = frame.data
        self.stream_ended = None
        self.end_stream = 'END_STREAM' in frame.flags
        self.priority = 'PRIORITY' in frame.flags
        self.encoding = encoding
        self.priority_updated = None

    def parse(self) -> Tuple[int, Dict[str, str]]:

        decoder = Decoder()
        decoder.header_table = self.hpack_table
        decoder.header_table_size = self.hpack_table.maxsize
        headers: List[Tuple[bytes, bytes]] = decoder.decode(self.raw_headers, raw=True)

        header_encoding = self.encoding
        if header_encoding:
            decoded_headers = []
            
            for header in headers:
                name, value = header
                decoded_headers.append(header.__class__(
                    name.decode(header_encoding),
                    value.decode(header_encoding)
                ))

            headers = decoded_headers

        status_code = None
        headers_dict = {}
        for k, v in headers:
            if k == b":status":
                status_code = int(v.decode("ascii", errors="ignore"))
            elif k.startswith(b":"):
                headers_dict[k.strip(b':')] = v
            else:
                headers_dict[k] = v
        
        return status_code, headers_dict

    