from __future__ import annotations
import socket
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType


class ARecordData(RecordData):
    '''A record'''

    def __init__(self, data: str):
        super().__init__(
            RecordType.A,
            data=data
        )

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, ARecordData]:
        
        ip = socket.inet_ntoa(
            data[cursor_position:cursor_position + size]
        )
        
        return cursor_position + size, ARecordData(ip)

    def dump(
        self, 
        names: Dict[str, int], 
        offset: int
    ) -> Iterable[bytes]:
        yield socket.inet_aton(self.data)
