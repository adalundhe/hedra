from typing import Coroutine, Dict, List, Union
from .types import RequestTypes


class BaseResult:

    __slots__ = (
        'action_id',
        'name',
        'checks',
        'error',
        'source',
        'user',
        'tags',
        'type',
        'time',
        'wait_start',
        'start',
        'connect_end',
        'write_end',
        'complete'
    )

    def __init__(
        self, 
        action_id: str,
        name: str, 
        source: str,
        user: str,
        tags: List[Dict[str, str]],
        type: Union[RequestTypes, str],
        checks: List[Coroutine], 
        error: Exception
    ) -> None:
        self.action_id = action_id
        self.name = name
        self.checks = checks
        self.error = error
        self.source = source
        self.user = user
        self.tags = tags
        self.type = type

        self.time = 0
        self.wait_start = 0
        self.start = 0
        self.connect_end = 0
        self.write_end = 0
        self.complete = 0

    def to_dict(self):
        return {
            'name': self.name,
            'error': str(self.error),
            'source': self.source,
            'user': self.user,
            'tags': self.tags,
            'type': self.type,
            'wait_start': float(self.wait_start),
            'start': float(self.start),
            'connect_end': float(self.connect_end),
            'write_end': float(self.write_end),
            'complete': float(self.complete)
        }