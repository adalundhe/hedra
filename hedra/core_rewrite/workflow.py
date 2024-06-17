import inspect
import os
import uuid
from typing import Any, Awaitable, Callable, Dict, List

import networkx

from .engines.client import Client, TimeParser
from .engines.client.config import Config
from .hooks import Hook


class Workflow:
    def __init__(self):
        self.graph = __file__
        self.name = self.__class__.__name__
        self.id = str(uuid.uuid4())

        self.context: Dict[str, Any] = {}
        self.hooks: Dict[str, Hook] = {
            name: hook
            for name, hook in inspect.getmembers(
                self, predicate=lambda member: isinstance(member, Hook)
            )
        }

        for hook in self.hooks.values():
            hook.call = hook.call.__get__(self, self.__class__)
            setattr(self, hook.name, hook.call)

        self.config = {
            "vus": 1000,
            "duration": "1m",
            "threads": os.cpu_count(),
            "connect_retries": 3,
        }

        self.config.update(
            {
                name: value
                for name, value in inspect.getmembers(self)
                if self.config.get(name)
            }
        )

        self.config["duration"] = TimeParser(self.config["duration"]).time

        self.client = Client(
            self.graph, self.id, self.name, self.id, Config(**self.config)
        )

        # self.client.set_mutations()

        self.workflow_graph = networkx.DiGraph()

        self.traversal_order: List[List[Callable[..., Awaitable[Any]]]] = []

        self.is_test = len([hook for hook in self.hooks.values() if hook.is_test]) > 0

        for hook in self.hooks.values():
            hook.setup(self.context)

        sources = []

        for hook_name, hook in self.hooks.items():
            self.workflow_graph.add_node(hook_name, hook=hook)

        for hook in self.hooks.values():
            if len(hook.dependencies) == 0:
                sources.append(hook.name)

            for dependency in hook.dependencies:
                self.workflow_graph.add_edge(dependency, hook.name)

        for traversal_layer in networkx.bfs_layers(self.workflow_graph, sources):
            self.traversal_order.append(
                [self.hooks.get(hook_name).call for hook_name in traversal_layer]
            )
