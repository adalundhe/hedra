from __future__ import annotations
import struct
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType
from .utils import (
    load_domain_name,
    pack_domain_name
)


class NAPTRRecordData(RecordData):
    '''A record'''

    def __init__(self, *args):
        super().__init__(
            RecordType.SRV,
            data=args
        )

        (
            order,
            preference,
            flags,
            service,
            regexp,
            replacement
        ) = args

        self.order = order
        self.preference = preference
        self.flags = flags
        self.service = service
        self.regexp = regexp
        self.replacement = replacement

    def __repr__(self):
        return '<%s-%s-%s: %s %s %s %s>' % (
            self.type_name, 
            self.order, 
            self.preference, 
            self.flags,
            self.service, 
            self.regexp, 
            self.replacement
        )


    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, NAPTRRecordData]:
        pos = cursor_position

        order, preference = struct.unpack('!HH', data[pos:pos + 4])
        pos += 4
        
        length = data[pos]
        pos += 1

        flags = data[pos:pos + length].decode()
        pos += length

        length = data[pos]
        pos += 1

        service = data[pos:pos + length].decode()
        pos += length

        length = data[pos]
        pos += 1

        regexp = data[pos:pos + length].decode()
        pos += length

        cursor_position, replacement = load_domain_name(data, pos)
        return cursor_position, NAPTRRecordData(
            order, 
            preference, 
            flags, 
            service, 
            regexp, 
            replacement
        )

    def dump(self, names: Dict[str, int], offset: int) -> Iterable[bytes]:
        raise NotImplementedError


