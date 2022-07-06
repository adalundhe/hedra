import gc
from types import FunctionType
from typing import Dict, List, Union
from hedra.core.engines.types.common.request import Request
from hedra.core.engines.types.playwright.command import Command


class Action:
    session=None
    timeout=0
    before_batch: FunctionType = None
    after_batch: FunctionType = None
    order=0
    weight=1
    
    def __init__(self) -> None:
        self.parsed: Union[Request, Command] = None
        self.before: FunctionType = None
        self.after: FunctionType = None
        self.checks: List[FunctionType] = []
        self.tags: List[Dict[str, str]] = []

    def to_type(self, name: str):
        raise NotImplementedError()