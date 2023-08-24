import time
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat
)
from typing import Any, Dict

class Entry(BaseModel):
    entry_id: StrictInt
    key: StrictStr
    value: Any
    term: StrictInt
    leader_host: StrictStr
    leader_port: StrictInt
    timestamp: StrictFloat=time.monotonic()

    def to_data(self):
        return {
            'key': self.key,
            'value': self.value,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_data(
        self,
        entry_id: int,
        leader_host: str,
        leader_port: int,
        term: int,
        data: Dict[str, Any]
    ):
        
        data_timestamp = data.get('timestamp')
        if data_timestamp is None:
            data['timestamp'] = time.monotonic()

        return Entry(
            entry_id=entry_id,
            leader_host=leader_host,
            leader_port=leader_port,
            term=term,
            **data
        )