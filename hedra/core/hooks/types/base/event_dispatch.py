from __future__ import annotations
import asyncio
from collections import OrderedDict, defaultdict
from typing import List, Union, Dict, Any, Tuple
from hedra.core.hooks.types.action.event import ActionEvent
from hedra.core.hooks.types.task.event import TaskEvent
from hedra.core.hooks.types.channel.event import ChannelEvent
from .event import BaseEvent
from .event_types import EventType
from .simple_context import SimpleContext


class EventDispatcher:

    def __init__(self, timeout: Union[int, float]=None) -> None:
        self.events: OrderedDict[EventType, List[BaseEvent]]= OrderedDict()
        self.priority_map = {
            EventType.CONTEXT: 0,
            EventType.LOAD: 1,
            EventType.EVENT: 2,
            EventType.TRANSFORM: 2,
            EventType.CONDITION: 2,
            EventType.SAVE: 3,
            EventType.ACTION: 4,
            EventType.TASK: 4,
            EventType.CHECK: 5,
            EventType.CHANNEL: 5,
            EventType.METRIC: 6
        }

        event_orderings = list(sorted(
            list(self.priority_map.items()),
            key=lambda event: event[1]
        ))

        for event_type_name, _ in event_orderings:
            self.events[event_type_name] = []
            
        self.events_by_name: Dict[str, BaseEvent] = {}
        self.timeout = timeout
        self.initial_events: Dict[str, List[BaseEvent]]= defaultdict(list)
        self.actions_and_tasks: Dict[str, Union[ActionEvent, TaskEvent]] = {}
        self.channels: Dict[str, ChannelEvent] = {}
        self.skip_list = []
        self.source_name = None

    def __iter__(self):
        for event_type in self.events:
            for event in self.events[event_type]:
                yield event

    def __getitem__(self, event_type: EventType):
        return self.events[event_type]
    
    def copy(self):
        dispatcher = EventDispatcher(timeout=self.timeout)

        for event in self.events_by_name.values():
            dispatcher.add_event(
                event.copy()
            )

        for event in self.events_by_name.values():
            event.context = SimpleContext()
            event.source.context = SimpleContext()

            for graph_event_name in event.events:

                if graph_event_name in dispatcher.events_by_name:
                    dispatcher.events_by_name[event.event_name].events[graph_event_name] = dispatcher.events_by_name.get(graph_event_name)

                else:
                    dispatcher.events_by_name[event.event_name].events[graph_event_name] = event.events.get(graph_event_name)

        for stage in self.initial_events:
            initial_event_names = [
                event.event_name for event in self.initial_events[stage]
            ]

            dispatcher.initial_events[stage] = [
                event for event in dispatcher.events_by_name.values() if event.event_name in initial_event_names
            ]

        dispatcher.assemble_action_and_task_subgraphs()

        return dispatcher

    def set_events(self, events: List[BaseEvent]) -> None:
        for event in events:
            self.events_by_name[event.event_name] = event
            self.events[event.event_type].append(event)

    def add_event(self, event: BaseEvent):

        if isinstance(event, (ActionEvent, TaskEvent)):

            self.actions_and_tasks[event.event_name] = event

        elif isinstance(event, ChannelEvent):
            self.channels[event.event_name] = event
            self.skip_list.append(event.event_name)

        self.events_by_name[event.event_name] = event
        self.events[event.event_type].append(event)

    def assemble_action_and_task_subgraphs(self):
        for event_name, event in self.actions_and_tasks.items():
            self.skip_list.append(event_name)

            for dependency_name in event.source.names:
                self.skip_list.append(dependency_name)
                
                dependency_event = self.events_by_name.get(dependency_name)
                # If we specify a Channel hook as a dependency of an Action
                # or Task - that Action/Task listens to the Channel.
                if isinstance(dependency_event, ChannelEvent):
                    event.source.is_listener = True
                    dependency_event.source.listeners.append(event)

                event.source.before.extend(
                    self._prepend_action_or_task_event(
                        dependency_event,
                        event
                    )
                )

        for event_name, event in self.events_by_name.items():
            for dependency_name in event.source.names:
                action_or_task_event = self.actions_and_tasks.get(dependency_name)
                if action_or_task_event:
                    self.skip_list.append(event_name)
                    action_or_task_event.source.after.extend(
                        self._append_action_or_task_event(
                            event,
                            action_or_task_event
                        )
                    )
                
                if isinstance(event, ChannelEvent):
                    action_or_task_event.source.is_notifier = True
                    action_or_task_event.source.channels.append(dependency_event)
                    event.source.notifiers.append(action_or_task_event)
                
                    
        for event in self.actions_and_tasks.values():
            for idx, layer in enumerate(event.before):
                event.source.before[idx] = [
                    self.events_by_name.get(event_name) for event_name in layer
                ]

            for idx, layer in enumerate(event.after):
                event.source.after[idx] = [
                    self.events_by_name.get(event_name) for event_name in layer
                ]

            for idx, layer in enumerate(event.checks):
                event.checks[idx] = [
                    self.events_by_name.get(event_name) for event_name in layer
                ]

        # Add listeners for a channel to the channel's notifiers
        for channel_event_name, channel_event in self.channels.items():
            for notifier in channel_event.source.notifiers:
                notifier.listeners.extend(channel_event.listeners)

            self.channels[channel_event_name] = channel_event

    async def dispatch_events(self, stage_name: str):

        await asyncio.gather(*[
            asyncio.create_task(
                self._execute_batch(initial_event)
            ) for initial_event in self.initial_events[stage_name] if initial_event.event_name not in self.skip_list
        ])        

    def _prepend_action_or_task_event(self, dependency_event: BaseEvent, action_or_task: BaseEvent):
        execution_path = list(dependency_event.execution_path)

        event_layer_found = False
        for idx, layer in enumerate(execution_path):

            if event_layer_found:
                dependency_event.execution_path.remove(layer)

            if action_or_task.event_name in layer:
                event_layer_found = True
                dependency_event.execution_path[idx].remove(action_or_task.event_name)

                if len(layer) < 1:
                    dependency_event.execution_path.remove(layer)

        return dependency_event.execution_path

    def _append_action_or_task_event(self, dependant_event: BaseEvent, action_or_task: BaseEvent):
        execution_path = []

        if dependant_event.event_type == EventType.CHECK:
            for idx, layer in enumerate(dependant_event.execution_path):
                if idx >= len(action_or_task.checks):
                    action_or_task.checks.append(layer)

                else:
                    action_or_task.checks[idx].extend([
                        node for node in layer if node not in action_or_task.checks[idx]
                    ])

        else:
            for idx, layer in enumerate(dependant_event.execution_path):
                if idx >= len(action_or_task.after):
                    action_or_task.after.append(layer)

                else:
                    action_or_task.after[idx].extend([
                        node for node in layer if node not in action_or_task.after[idx]
                    ])

        return execution_path

    async def _execute_batch(self, initial_event: BaseEvent):
        for layer in initial_event.execution_path:
                layer_events = [
                    self.events_by_name.get(event_name) for event_name in layer
                ]

                results: List[Dict[str, Any]] = await asyncio.gather(*[
                    asyncio.create_task(
                        asyncio.wait_for(
                            event.call(**event.next_args),
                            timeout=self.timeout
                        ) if self.timeout else event.call()
                    ) for event in layer_events
                ])

                result_events: List[Tuple[BaseEvent, Any]] = []
                for result in results:
                    for event_name, result in result.items():
                        event = self.events_by_name.get(event_name)
                        result_events.append((event, result))

                for event, result in result_events:
                    next_events = [
                        event.events.get(event_name) for event_name in  event.next_map if event.events.get(event_name) is not None
                    ]

                    event.context.update(event.next_args[event.event_name])
                    event.source.stage_instance.context.update(event.next_args[event.event_name])
                

                    for next_event in next_events:
                        next_event.context.update(event.context)

                        next_event.next_args[next_event.event_name].update(result)
                        event.context.update(next_event.next_args[next_event.event_name])
                        event.source.stage_instance.context.update(next_event.next_args[next_event.event_name])

                
