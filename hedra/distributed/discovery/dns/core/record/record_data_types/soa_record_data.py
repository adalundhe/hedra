from __future__ import annotations
import socket
import struct
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType
from .utils import (
    load_domain_name,
    pack_domain_name
)


class SOARecordData(RecordData):

    def __init__(self, *args):
        super().__init__(
            RecordType.SOA,
            data=args
        )

        (
            mname,
            rname,
            serial,
            refresh,
            retry,
            expire,
            minimum,
        ) = args

        self.mname = mname
        self.rname = rname
        self.serial = serial
        self.refresh = refresh
        self.retry = retry
        self.expire = expire
        self.minimum = minimum

    def __repr__(self):
        return '<%s: %s>' % (self.type_name, self.rname)


    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, SOARecordData]:
        
        cursor_position, mname = load_domain_name(data, cursor_position)
        cursor_position, rname = load_domain_name(data, cursor_position)

        (
            serial,
            refresh,
            retry,
            expire,
            minimum,
        ) = struct.unpack(
            '!LLLLL', 
            data[cursor_position:cursor_position + 20]
        )

        return cursor_position + 20, SOARecordData(
            mname, 
            rname, 
            serial, 
            refresh, 
            retry, 
            expire,
            minimum
        )

    def dump(self, names: Dict[str, int], offset: int) -> Iterable[bytes]:

        mname = pack_domain_name(
            self.mname, 
            names, 
            offset + 2
        )

        mname_length = len(mname)

        domain_name = pack_domain_name(
            self.rname, 
            names, 
            offset + 2 + mname_length
        )

        record_bytes = struct.pack(
            '!LLLLL', 
            self.serial, 
            self.refresh, 
            self.retry,
            self.expire, 
            self.minimum
        )

        record_data = [
            mname,
            domain_name, 
            record_bytes
        ]

        for data in record_data:
            yield data
