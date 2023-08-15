import io
import struct
import time
from .query_type import QueryType
from typing import Dict, Tuple
from .record_data_types import (
    ARecordData,
    AAAARecordData,
    CNAMERecordData,
    MXRecordData,
    NAPTRRecordData,
    NSRecordData,
    PTRRecordData,
    RecordData,
    RecordType,
    RecordTypesMap,
    SOARecordData,
    SRVRecordData,
    TXTRecordData,
    UnsupportedRecordData

)
from hedra.distributed.discovery.dns.core.record.record_data_types.utils import (
    load_domain_name, 
    pack_domain_name, 
    pack_string
)


MAXAGE = 3600000



class Record:

    record_types: Dict[
        RecordType,
        RecordData
    ] = {
        RecordType.A: ARecordData,
        RecordType.AAAA: AAAARecordData,
        RecordType.CNAME: CNAMERecordData,
        RecordType.MX: MXRecordData,
        RecordType.NAPTR: NAPTRRecordData,
        RecordType.NS: NSRecordData,
        RecordType.PTR: PTRRecordData,
        RecordType.SOA: SOARecordData,
        RecordType.SRV: SRVRecordData,
        RecordType.TXT: TXTRecordData
    }

    def __init__(
        self,
        query_type: QueryType = QueryType.REQUEST,
        name: str = '',
        record_type: RecordType = RecordType.ANY,
        qclass: int = 1,
        ttl: int = 0,
        data: Tuple[int, RecordData] = None
    ):
        self.query_type = query_type
        self.name = name
        self.record_type = record_type
        self.qclass = qclass

        self.ttl = ttl  # 0 means item should not be cached
        self.data = data
        self.timestamp = int(time.time())

        self.types_map = RecordTypesMap()

    @classmethod
    def create_rdata(
        cls,
        record_type: RecordType, 
        *args
    ) -> RecordData:
        
        record_data = cls.record_types.get(record_type)

        if record_data is None:
            return UnsupportedRecordData(
                record_type,
                *args
            )

        return record_data(
            *args
        )

    @classmethod
    def load_rdata(
        cls,
        record_type: RecordType, 
        data: bytes, 
        cursor_position: int,
        size: int
    ) -> Tuple[int, RecordData]:
        '''Load RData from a byte sequence.'''
        record_data = cls.record_types.get(record_type)
        if record_data is None:
            return UnsupportedRecordData.load(
                data, 
                cursor_position, 
                size, 
                record_type
            )
        
        return record_data.load(
            data, 
            cursor_position, 
            size
        )

    def copy(
        self, 
        **kwargs
    ):
        return Record(
            query_type=kwargs.get('query_type', self.query_type),
            name=kwargs.get('name', self.name),
            record_type=kwargs.get('record_type', self.record_type),
            qclass=kwargs.get('qclass', self.qclass),
            ttl=kwargs.get('ttl', self.ttl),
            data=kwargs.get('data', self.data)
        )

    def parse(
        self, 
        data: bytes, 
        cursor_position: int
    ):
        cursor_position, self.name = load_domain_name(
            data, 
            cursor_position
        )

        record_type, self.qclass = struct.unpack(
            '!HH', 
            data[cursor_position:cursor_position + 4]
        )

        self.record_type = self.types_map.types_by_code.get(
            record_type
        )

        cursor_position += 4
        if self.query_type == QueryType.RESPONSE:
            self.timestamp = int(time.time())
            self.ttl, size = struct.unpack(
                '!LH', 
                data[cursor_position:cursor_position + 6]
            )

            cursor_position += 6

            _, self.data = Record.load_rdata(
                self.record_type, 
                data, 
                cursor_position, 
                size
            )

            cursor_position += size

        return cursor_position

    def pack(
        self, 
        names, 
        offset=0
    ):
        buf = io.BytesIO()

        buf.write(
            pack_domain_name(
                self.name, 
                names, 
                offset
            )
        )

        buf.write(
            struct.pack(
                '!HH', 
                self.record_type.value, 
                self.qclass
            )
        )

        if self.query_type == QueryType.RESPONSE:

            if self.ttl < 0:
                ttl = MAXAGE

            else:
                now = int(time.time())
                self.ttl -= now - self.timestamp

                if self.ttl < 0:
                    self.ttl = 0

                self.timestamp = now
                ttl = self.ttl

            buf.write(
                struct.pack(
                    '!L', 
                    ttl
                )
            )

            data_str = b''.join(
                self.data.dump(
                    names, 
                    offset + buf.tell()
                )
            )

            buf.write(
                pack_string(
                    data_str, 
                    '!H'
                )
            )

        return buf.getvalue()
