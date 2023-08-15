from __future__ import annotations
from typing import Tuple, Iterable, Dict
from .record_data import RecordData
from .record_types import RecordType


class UnsupportedRecordData(RecordData):
    '''Unsupported RData'''
    def __init__(self, rtype: RecordType, raw: str):

        super().__init__(
            rtype,
            data=raw.encode()
        )

        self.raw = raw

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int,
        record_type: RecordType
    ) -> Tuple[int, UnsupportedRecordData]:
        
        return cursor_position + size, UnsupportedRecordData(
            record_type, 
            data[cursor_position:cursor_position + size]
        )

    def dump(self, names: Dict[str, int], offset: int) -> Iterable[bytes]:
        yield self.raw