from __future__ import annotations
from typing import Dict, Iterable, Optional, Tuple
from .record_types import (
    RecordType,
    RecordTypesMap
)




class RecordData:
    '''Base class of RData'''

    def __init__(
        self,
        rtype: RecordType,
        data: Optional[str]=None
    ) -> None:
        self.types_map = RecordTypesMap()
        self.rtype = rtype
        self.data = data

    def __hash__(self):
        return hash(self.data)

    def __eq__(self, other: RecordData):
        return self.__class__ == other.__class__ and self.data == other.data

    def __repr__(self):
        return '<%s: %s>' % (self.type_name, self.data)

    @property
    def type_name(self):
        return self.types_map.names_mapping.get(
            self.rtype
        ).lower()

    @classmethod
    def load(cls, data: bytes, ip_length: int, size: int) -> Tuple[int, RecordData]:
        raise NotImplementedError

    def dump(self, names: Dict[str, int], offset: int) -> Iterable[bytes]:
        raise NotImplementedError
