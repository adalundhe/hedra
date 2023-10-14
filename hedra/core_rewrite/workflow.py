import inspect
import os
import uuid
from hedra.core.engines.client import Client
from hedra.core.engines.client.config import Config
from typing import (
    Dict,
    Any
)
from .hooks import (
    Hook
)


class Workflow:

    def __init__(self):
        self.graph = __file__
        self.name = self.__class__.__name__
        self.id = str(uuid.uuid4())

        self.context: Dict[str, Any] = {}
        self.hooks: Dict[
            str,
            Hook
        ] = {
            name: hook for name, hook in inspect.getmembers(
                self, 
                predicate=lambda member: isinstance(member, Hook)
            )
        }

        self.config = {
            'vus': 1000,
            'duration': '1m',
            'threads': os.cpu_count()
        }

        self.config.update({
            name: value for name, value in inspect.getmembers(
                self
            ) if self.config.get(name)
        })

        self.client = Client(
            self.graph,
            self.id,
            self.name,
            self.id,
            Config(**self.config)
        )

        