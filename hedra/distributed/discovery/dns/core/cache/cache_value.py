import time
from hedra.distributed.discovery.dns.core.record.record_data_types import (
    RecordData
)
from hedra.distributed.discovery.dns.core.record import (
    Record, 
    RecordType
)
from typing import Dict, Iterable, Tuple


class CacheValue:
    def __init__(self):
        self.data: Dict[RecordData, Dict[Tuple[int, RecordData], Record]] = {}

    def check_ttl(self, record: Record):
        return record.ttl < 0 or record.timestamp + record.ttl >= time.time()

    def get(
        self, 
        record_type: RecordType
    ) -> Iterable[Record]:

        if record_type == RecordType.ANY:
            for qt in self.data.keys():
                yield from self.get(qt)
        
        results = self.data.get(record_type)
        if results is not None:

            keys = list(results.keys())
            for key in keys:
                record = results[key]

                if self.check_ttl(record):
                    yield record
                    
                else:
                    results.pop(key, None)

    def add(self, record: Record):
        if self.check_ttl(record):
            results = self.data.setdefault(record.record_type, {})
            results[record.data] = record

