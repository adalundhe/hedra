from __future__ import annotations
import struct
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType
from .utils import (
    load_domain_name,
    pack_domain_name
)


class MXRecordData(RecordData):
    '''A record'''

    def __init__(self, *args):
        super().__init__(
            RecordType.MX,
            data=args
        )

        (
            preference,
            exchange
        ) = args

        self.preference = preference
        self.exchange = exchange

    def __repr__(self):
        return '<%s-%s: %s>' % (self.type_name, self.preference, self.exchange)

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, MXRecordData]:
        
        preference, = struct.unpack(
            '!H', 
            data[cursor_position:cursor_position + 2]
        )

        cursor_position, exchange = load_domain_name(data, cursor_position + 2)

        return cursor_position, MXRecordData(preference, exchange)

    def dump(self, names: Dict[str, int], offset: int) -> Iterable[bytes]:

        preference = struct.pack(
            '!H', 
            self.preference
        )

        domain_name = pack_domain_name(
            self.exchange, 
            names, 
            offset + 4
        ) 

        record_data = [
            preference,
            domain_name
        ]

        for data in record_data:
            yield data

