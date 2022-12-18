import uuid
from typing import Any, Coroutine, Dict, Iterator, Union, List
from .metadata import Metadata
from .hooks import Hooks
from .types import ProtocolMap

class BaseAction:

    __slots__ = ( 
        'action_id'
        'protocols', 
        'name', 
        'is_setup', 
        'metadata', 
        'hooks'
    )

    def __init__(
        self, 
        name: str=None,
        user: str=None, 
        tags: List[Dict[str, str]] = []
    ) -> None:
        self.action_id = str(uuid.uuid4())
        self.protocols = ProtocolMap()
        self.name = name
        self.is_setup = False
        self.metadata = Metadata(user, tags)
        self.hooks: Hooks[BaseAction] = Hooks()