from __future__ import annotations
from typing import Dict, Iterable, Tuple, Optional
from .record_data import RecordData
from .record_types import RecordType
from .utils import (
    pack_domain_name
)


class DomainRecordData(RecordData):
    '''A record'''

    def __init__(
        self, 
        record_type: RecordType,
        data: Optional[str]=None
    ):
        super().__init__(
            record_type,
            data=data
        )

    @classmethod
    def load(
        cls, 
        data: bytes, 
        cursor_position: int,
        size: int
    ) -> Tuple[int, DomainRecordData]:
        raise NotImplementedError('Err. - Not implemented for DomainRecordData type')

    def dump(
        self, 
        names: Dict[str, int], 
        offset: int
    ) -> Iterable[bytes]:
        yield pack_domain_name(self.data, names, offset + 2)


