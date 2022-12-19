from typing import Any
from hedra.core.graphs.hooks.types.hook import Hook


class Event:

    def __init__(self, target: Hook, source: Hook) -> None:
        self.target = target
        self.source = source
        self.target_name = self.target.name
        self.target_shortname = self.target.shortname

    def __getattribute__(self, name: str) -> Any:
        target = object.__getattribute__(self, 'target')
        if hasattr(target, name) and name != 'call':
            return getattr(target, name)

        return object.__getattribute__(self, name)

    async def call(self, *args, **kwargs):
        result = await self.target.call(*args, **kwargs)
        await self.source.call(result)

        return result