from __future__ import annotations
from enum import Enum
from typing import Dict, Optional
'''
Constants of DNS types.
'''

class RecordType(Enum):
    NONE = 0
    A = 1
    NS = 2
    CNAME = 5
    SOA = 6
    PTR = 12
    MX = 15
    TXT = 16
    AAAA = 28
    SRV = 33
    NAPTR = 35
    ANY = 255
        

class RecordTypesMap:

    def __init__(self) -> None:
        self.names_mapping: Dict[RecordType, str] = {}
        self.codes_mapping: Dict[RecordType, int] = {}
        self.types_by_code: Dict[int, RecordType] = {}
        self.types_by_name: Dict[str, RecordType] = {}

        for record_type in RecordType:
            self.names_mapping[record_type] = record_type.name
            self.codes_mapping[record_type] = record_type.value
            self.types_by_code[record_type.value] = record_type
            self.types_by_name[record_type.name] = record_type


    def get_name_by_code(
        self,
        code: int, 
        default: Optional[RecordType]=None
    ) -> str:
        record_type = self.types_by_code.get(
            code, 
            default
        )

        if record_type is None:
            return str(code)

        return record_type.name
    
    def get_code_by_name(
        self,
        name: str,
        default: Optional[RecordType]=None
    ):
        record_type = self.types_by_name.get(
            name, 
            default
        )

        if record_type is None:
            raise KeyError(f'No record type matches code - {name}')

        return record_type.value