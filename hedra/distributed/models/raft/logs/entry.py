from hedra.distributed.snowflake import Snowflake
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)
from typing import Any, Dict, Union

class Entry(BaseModel):
    entry_id: StrictInt
    key: StrictStr
    value: Any
    term: StrictInt
    leader_host: StrictStr
    leader_port: StrictInt
    timestamp: StrictInt

    def __init__(self, *args, **kwargs):

        entry_id: Union[int, None] = kwargs.get('entry_id')
        if entry_id:
            kwargs['timestamp'] = Snowflake.parse(entry_id).timestamp

        super().__init__(
            *args,
            **kwargs
        )

    def to_data(self):
        return {
            'key': self.key,
            'value': self.value,
            'timestamp': self.timestamp
        }
    
    @classmethod
    def from_data(
        cls,
        entry_id: int,
        leader_host: str,
        leader_port: int,
        term: int,
        data: Dict[str, Any]
    ):

        return Entry(
            entry_id=entry_id,
            leader_host=leader_host,
            leader_port=leader_port,
            term=term,
            **data
        )