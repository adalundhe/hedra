from typing import Any
from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook


class Event:

    def __init__(self, target: Hook, source: EventHook) -> None:
        self.target = target
        self.source = source
        self.target_name = None
        self.target_shortname = None

        if target:
            self.target_name = self.target.name
            self.target_shortname = self.target.shortname

        self.pre = source.pre
        self.as_hook = False

    def __getattribute__(self, name: str) -> Any:
        target = object.__getattribute__(self, 'target')
        source = object.__getattribute__(self, 'source')
        if hasattr(target, name) and name != 'call':
            return getattr(target, name)

        elif target is None and hasattr(source, name) and name != 'call':
            return getattr(source, name)

        return object.__getattribute__(self, name)

    async def call(self, *args, **kwargs):
        if self.source.pre is True and self.target:
            await self.source.call()
            result = await self.target.call(*args, **kwargs)

        elif self.source.pre is False and self.target:
            result = await self.target.call(*args, **kwargs)
            await self.source.call(result)

        else:
            result = await self.source.call()

        return result