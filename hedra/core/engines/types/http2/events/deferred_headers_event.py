from typing import Dict, List, Tuple, Union
from hedra.core.engines.types.common.decoder import Decoder
from hedra.core.engines.types.common.encoder import Encoder
from .base_event import BaseEvent


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
        headers: List[Tuple[bytes, bytes]] = {}

        try:
            headers = decoder.decode(self.raw_headers, raw=True)

        except Exception:
            return 400, {}

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

    