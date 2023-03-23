from __future__ import annotations
import networkx
from collections import defaultdict
from typing import List, Union, Dict
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.hook import Hook, HookType
from .event_dispatch import EventDispatcher
from .get_event import get_event

class EventGraph:

    def __init__(self, hooks_by_type: Dict[HookType, Dict[str, Hook]]) -> None:
        self.hooks_by_type = hooks_by_type
        self.hooks_by_name = {}
        self.hooks_by_shortname: Dict[str, Dict[str, Hook]] = defaultdict(dict)

        for hook_type in self.hooks_by_type:
            for hook in self.hooks_by_type[hook_type].values():
                self.hooks_by_name[hook.name] = hook
                self.hooks_by_shortname[hook.stage][hook.shortname] = hook

        self.hooks_graph = networkx.DiGraph()

        self.hooks_graph.add_nodes_from([
            (
                hook_name,
                {'hook': hook}
            ) for hook_name, hook in self.hooks_by_name.items()
        ])

        self.event_hooks: List[Hook] = [
            *list(self.hooks_by_type.get(
                HookType.ACTION, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.CHANNEL,
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.CHECK,
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.CONDITION, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.CONTEXT, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.EVENT, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.LOAD,
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.METRIC,
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.SAVE,
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.TASK, 
                {}
            ).values()),
            *list(self.hooks_by_type.get(
                HookType.TRANSFORM, 
                {}
            ).values()),
        ]
        
        nodes = [self.hooks_by_name.get(node) for node in self.hooks_graph.nodes()]
        self.events: Dict[str, BaseEvent] = {}
        self.base_stages = list(set([node.stage_instance.__class__.__base__.__name__ for node in nodes]))
        self.removal_targets = []
        self.action_and_task_events = {}

    def hooks_to_events(self) -> EventGraph:
        for event_hook in self.event_hooks:
            for idx, target_hook_name in enumerate(event_hook.names): 
                target: Union[Hook, None] = self.hooks_by_name.get(target_hook_name)

                if target is None:
                    target: Hook = self.hooks_by_shortname[event_hook.stage].get(target_hook_name)
                    event_hook.names[idx] = target.name
   
                event = get_event(target, event_hook)
                
                if isinstance(target, Hook):

                    target: BaseEvent = get_event(target, target)
                    self.events[target.event_name] = target

                    self.hooks_graph.update(nodes=[(
                        target.event_name,
                        {'hook': target}
                    )])

  
                self.events[event.event_name] = event

                self.hooks_graph.update(nodes=[(
                    event.event_name,
                    {'hook': event}
                )])

            if self.events.get(event_hook.name) is None:

                event = get_event(event_hook, event_hook)

                self.events[event_hook.name] = event
                self.hooks_graph.update(nodes=[(
                    event_hook.name,
                    {'hook': event}
                )])

        return self

    def assemble_graph(self) -> EventGraph:      
        for event_hook in self.event_hooks:  
            for target_hook_name in event_hook.names: 
                target: BaseEvent = self.events.get(target_hook_name)

                if target is None:
                    target_hook: Hook = self.hooks_by_shortname[event_hook.stage].get(target_hook_name)
                    target_name = target_hook.name

                else:
                    target_name = target.event_name

                self.hooks_graph.add_edge(target_name, event_hook.name)


        return self

    def apply_graph_to_events(self) -> None:

        for event in self.events.values():

            event.execution_path = [edge for edge in networkx.bfs_layers(self.hooks_graph, event.event_name)]
            event.previous_map = [node for node in self.hooks_graph.predecessors(event.event_name)]
            event.next_map = [node for node in self.hooks_graph.successors(event.event_name)]

            for path_layer in event.execution_path:
                event.events.update({
                    event_name: self.events.get(event_name) for event_name in path_layer
                })

            hook_names = [hook.name for hook in event.source.stage_instance.hooks[event.hook_type]]

            if event.event_name in hook_names:
                hook_idx = hook_names.index(event.event_name)
                event.source.stage_instance.hooks[event.hook_type][hook_idx] = event

            else:
                event.source.stage_instance.hooks[event.hook_type].append(event)  

            if event.source.stage_instance.dispatcher is None:
                event.source.stage_instance.dispatcher = EventDispatcher()
                
            if len(event.previous_map) < 1 and event.event_name in hook_names:

                event.source.stage_instance.dispatcher.source_name = event.stage
                event.source.stage_instance.dispatcher.initial_events[event.source.stage].append(event)
            
            for layer in event.execution_path:
                for event_name in layer:
                    next_event = self.events.get(event_name)
                    event.source.stage_instance.dispatcher.add_event(next_event)

            
            event.source.stage_instance.dispatcher.add_event(event)
        
        # networkx.draw_networkx(self.hooks_graph, pos=networkx.spring_layout(self.hooks_graph), with_labels=True, arrows=True)
        # plt.show()