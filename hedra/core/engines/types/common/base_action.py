import uuid
from typing import Any, Dict, List, Any, TypeVar, Generic
from .metadata import Metadata
from .hooks import Hooks
from .types import ProtocolMap


A = TypeVar('A')


class BaseAction(Generic[A]):

    __slots__ = ( 
        'action_id'
        'protocols', 
        'name', 
        'is_setup', 
        'metadata', 
        'hooks',
        'event',
        'action_args'
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
        self.event = None
        self.action_args: Dict[str, Any] = {}