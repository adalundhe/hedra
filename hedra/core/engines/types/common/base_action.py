from types import FunctionType
from typing import Any, Coroutine, Dict, Iterator, Union, List
from .metadata import Metadata
from .hooks import Hooks
from .types import ProtocolMap

class BaseAction:

    def __init__(
        self, 
        name: str=None,
        user: str=None, 
        tags: List[Dict[str, str]] = [],  
        checks: List[FunctionType] = None, 
        hooks: Dict[str, Coroutine] = {},
    ) -> None:
        self.protocols = ProtocolMap()

        self.name = name
        self.is_setup = False
        self.checks = checks
        self.metadata = Metadata(user, tags)
        self.hooks = Hooks(**hooks)