from __future__ import annotations
from typing import Tuple
from .domain_record_data import DomainRecordData
from .record_types import RecordType
from .utils import load_domain_name


class NSRecordData(DomainRecordData):

    def __init__(self, data: str):
        super().__init__(
            RecordType.NS,
            data=data
        )

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int,
        size: int
    ) -> Tuple[int, NSRecordData]:
        
        cursor_position, domain = load_domain_name(
            data,
            cursor_position
        )

        return cursor_position, NSRecordData(domain)