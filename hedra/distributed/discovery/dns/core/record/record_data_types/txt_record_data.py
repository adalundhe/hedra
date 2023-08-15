from __future__ import annotations
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType
from .utils import (
    load_string,
    pack_string
)


class TXTRecordData(RecordData):
    '''A record'''

    def __init__(self, data: str):
        super().__init__(
            RecordType.TXT,
            data=data
        )

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, TXTRecordData]:
        
        _, text = load_string(
            data, 
            cursor_position
        )

        return cursor_position + size, TXTRecordData(
            text.decode()
        )

    def dump(
        self, 
        names: Dict[str, int], 
        offset: int
    ) -> Iterable[bytes]:
        yield pack_string(self.data)
