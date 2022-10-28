from __future__ import annotations
import inspect
from typing import Any, Generic, TypeVar
from hedra.core.engines.types.common.base_action import BaseAction
from typing import Dict, List
from hedra.core.engines.types.common.types import RequestTypes

A = TypeVar('A')

class Action(BaseAction, Generic[A]):

    __slots__ = (
        "source",
        "use_security_context",
        "security_context",
        "type"
    )


    def __init__(self, 
        name: str=None,
        source: str=None,
        user: str=None, 
        tags: List[Dict[str, str]] = [],
        use_security_context: bool = False
    ) -> Any:

        self.source = source
        self.use_security_context = use_security_context
        self.security_context: Any = None
        self.type = A
        self.plugin_type = None

        super().__init__(
            name,
            user,
            tags
        )

    def setup(self):
        pass

    def to_serializable(self):
        attributes = inspect.getmembers(
            self, 
            lambda attr: not(inspect.isroutine(attr))
        )

        instance_attributes = [
            attr for attr in attributes if not(
                attr[0].startswith('__') and attr[0].endswith('__')
            )
        ]

        base_attributes = set(dir(Action) + dir(BaseAction))

        serializable = {
            'name': self.name,
            'type': str(self.type),
            'plugin_type': self.plugin_type,
            'use_security_context': self.use_security_context,
            'security_context': self.security_context,
            'metadata': {
                'user': self.metadata.user,
                'tags': self.metadata.tags
            },
            'hooks': self.hooks.to_names(),
            'fields': {}
        }
        for attribute_name, attribute_value in instance_attributes:
            
            if serializable.get(attribute_name) is None and attribute_name not in base_attributes:
                serializable['fields'][attribute_name] = attribute_value

        return serializable