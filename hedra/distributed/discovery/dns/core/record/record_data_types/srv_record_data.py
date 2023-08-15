from __future__ import annotations
import struct
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType
from .utils import (
    load_domain_name,
    pack_domain_name
)


class SRVRecordData(RecordData):
    '''A record'''

    def __init__(self, *args):
        super().__init__(
            RecordType.SRV,
            data=args
        )

        (
            priority,
            weight,
            port,
            hostname
        ) = args

        self.priority = priority
        self.weight = weight
        self.port = port
        self.hostname = hostname

    def __repr__(self):
        return '<%s-%s: %s:%s>' % (
            self.type_name, 
            self.priority,
            self.hostname, 
            self.port
        )


    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, SRVRecordData]:
        
        priority, weight, port = struct.unpack(
            '!HHH', 
            data[cursor_position:cursor_position + 6]
        )

        cursor_position, hostname = load_domain_name(
            data, 
            cursor_position + 6
        )

        return cursor_position, SRVRecordData(
            priority, 
            weight, 
            port, 
            hostname
        )

    def dump(
        self, 
        names: Dict[str, int], 
        offset: int
    ) -> Iterable[bytes]:
        
        record_bytes = struct.pack(
            '!HHH', 
            self.priority, 
            self.weight, 
            self.port
        )

        domain_name = pack_domain_name(
            self.hostname, 
            names, 
            offset + 8
        )

        record_data = [
            record_bytes,
            domain_name
        ]

        for data in record_data:
            yield data

