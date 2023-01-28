from __future__ import annotations
import networkx
from typing import List, Union,Dict
from networkx.exception import NetworkXError
from hedra.core.graphs.events import get_event
from hedra.core.graphs.events.base_event import BaseEvent
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook, 
    TransformHook,
    ContextHook,
    ConditionHook
)
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook, HookType


class EventGraph:

    def __init__(self, hooks_by_type: Dict[HookType, Dict[str, Hook]]) -> None:
        self.hooks_by_type = hooks_by_type
        self.hooks_by_name = {}

        for hook_type in self.hooks_by_type:
            for hook in self.hooks_by_type[hook_type].values():
                self.hooks_by_name[hook.name] = hook

        self.hooks_graph = networkx.DiGraph()

        self.hooks_graph.add_nodes_from([
                (
                    hook_name,
                    {'hook': hook}
                ) for hook_name, hook in self.hooks_by_name.items()
            ])
            

        self.event_hooks: List[Union[EventHook, TransformHook, ContextHook, ConditionHook]] = [
            *list(self.hooks_by_type.get(
                HookType.EVENT, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.TRANSFORM, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.CONDITION, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.CONTEXT, 
                {}
            ).values())
        ]
        
        self.events: Dict[str, BaseEvent] = {}


    def hooks_to_events(self) -> EventGraph:
        for event_hook in self.event_hooks:
            
            for target_hook_name in event_hook.names: 
                target = self.hooks_by_name.get(target_hook_name)

                event = get_event(target, event_hook)
        
                if isinstance(target, Hook):


                    target = get_event(target, target)
                    target.pre_sources = {}
                    target.post_sources = {}

                    self.hooks_by_name[target.event_name] = target
                    self.events[target.event_name] = target

                    self.hooks_graph.update(nodes=[(
                        target.event_name,
                        {'hook': target}
                    )])

  
                self.hooks_by_name[event_hook.name] = event
                self.events[event.event_name] = event

                self.hooks_graph.update(nodes=[(
                    event.event_name,
                    {'hook': event}
                )])

        return self

    def assemble_graph(self) -> EventGraph:
        
        
        for event_hook in self.event_hooks:  
            for target_hook_name in event_hook.names: 
                event: BaseEvent = self.events.get(event_hook.name)
                target: BaseEvent = self.events.get(target_hook_name)

                target_name = event.target.name
                if isinstance(event.target, BaseEvent):
                    target_name = event.target.event_name


                if target_name == target.target.name and event.pre:
                    self.hooks_graph.add_edge(event.event_name, target_name)

                
                elif target_name == target.target.name and event.pre is False:
                    self.hooks_graph.add_edge(target_name, event.event_name)

                elif target_name == target.event_name and event.pre:
                    self.hooks_graph.add_edge(event.event_name, target.event_name)

                
                elif target_name == target.event_name and event.pre is False:
                    self.hooks_graph.add_edge(target.event_name, event.event_name)


        for event_hook in self.event_hooks:  
            for target_hook_name in event_hook.names: 
                event: BaseEvent = self.hooks_by_name.get(event_hook.name)
                target: BaseEvent = self.hooks_by_name.get(target_hook_name)
                
                target_name = event.target.name
                if isinstance(event.target, BaseEvent):
                    target_name = event.target.event_name

     
                if target_name == target.event_name and event.pre:
                    try:
                        self.hooks_graph.remove_edge(target.target.name, target_name)
                    except NetworkXError:
                        pass

                    self.hooks_graph.add_edge(target.target.name, event.event_name)

            

        return self

    def apply_graph_to_events(self) -> None:

        for event in self.events.values():
            event.execution_path = [edge for edge in networkx.bfs_layers(self.hooks_graph, event.event_name)]
            event.previous_map[event.event_name] = [node for node in self.hooks_graph.predecessors(event.event_name)]
            event.next_map[event.event_name] = [node for node in self.hooks_graph.successors(event.event_name)]

            for path_layer in event.execution_path:
                event.events.update({
                    event_name: self.events.get(event_name) for event_name in path_layer
                })

            if event.event_name == event.target.name:
                hook_names = [hook.name for hook in event.stage_instance.hooks[event.hook_type]]
                hook_idx = hook_names.index(event.event_name)

                event.stage_instance.hooks[event.hook_type][hook_idx] = event

        