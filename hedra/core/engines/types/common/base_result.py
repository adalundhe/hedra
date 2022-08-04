from typing import Coroutine, Dict, List
from .types import RequestTypes


class BaseResult:

    def __init__(
        self, 
        name: str, 
        source: str,
        user: str,
        tags: List[Dict[str, str]],
        type: RequestTypes,
        checks: List[Coroutine], 
        error: Exception
    ) -> None:
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
        self.read_end = 0