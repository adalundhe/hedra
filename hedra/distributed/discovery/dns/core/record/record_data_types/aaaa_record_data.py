from __future__ import annotations
import socket
from typing import Dict, Iterable, Tuple
from .record_data import RecordData
from .record_types import RecordType


class AAAARecordData(RecordData):

    def __init__(self, data: str):
        super().__init__(
            RecordType.AAAA,
            data=data
        )

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int, 
        size: int
    ) -> Tuple[int, AAAARecordData]:
        
        ip = socket.inet_ntop(
            socket.AF_INET6, 
            data[cursor_position:cursor_position + size]
        )
        
        return cursor_position + size, AAAARecordData(ip)

    def dump(
        self, 
        names: Dict[str, int], 
        offset: int
    ) -> Iterable[bytes]:
        yield socket.inet_pton(socket.AF_INET6, self.data)
