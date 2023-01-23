from typing import Any, Dict
from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook


class Event:

    def __init__(self, target: Hook, source: EventHook) -> None:

        target.is_event = True

        self.target = target
        self.pre_sources: Dict[str, EventHook] = {}
        self.post_sources: Dict[str, EventHook] = {}
        self.target_name = None
        self.target_shortname = None
        self.target_key = None

        if source.pre:
            self.pre_sources[source.name] = source

        else:
            self.post_sources[source.name] = source

        if target:
            self.target_name = self.target.name
            self.target_shortname = self.target.shortname

        self.pre = source.pre
        self.as_hook = False

    def __getattribute__(self, name: str) -> Any:
        
        target = object.__getattribute__(self, 'target')

        if target and hasattr(target, name) and name != 'call':
            return getattr(target, name)
        
        return object.__getattribute__(self, name)

    async def call(self, *args, **kwargs):

        result = None

        if self.target:
            
            result = await self.execute_pre()
            result = await self.target.call(*args, **kwargs) 
            result = await self.execute_post(result)

        else:

            for source in self.pre_sources.values():
                result = await source.call()

                if source.key:
                    source.stage_instance.context[source.key] = result


            for source in self.post_sources.values():
                result = await source.call()

                if source.key:
                    source.stage_instance.context[source.key] = result

        return result

    async def execute_pre(self):

        result = None

        for source in self.pre_sources.values():
            result = await source.call()

            if source.key:
                source.stage_instance.context[source.key] = result
                self.target.stage_instance.context[source.key] = result

        return result

    async def execute_post(self, result: Any):
        for source in self.post_sources.values():
            source_hook_input = result
            if source.key:
                source_hook_input = self.target.stage_instance.context[source.key]

                if source_hook_input is None:
                    source_hook_input = source.stage_instance.context[source.key]

                if source_hook_input is None:
                    source_hook_input = result

            source_result = await source.call(source_hook_input)

            if source.key:
                source.stage_instance.context[source.key] = source_result
                self.target.stage_instance.context[source.key] = source_result

            else:
                result = source_result
        
        return result