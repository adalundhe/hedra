from typing import Any, Dict, Type, Callable, Awaitable, Any, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.simple_context import SimpleContext
from .hook import Hook
from .hook_metadata import HookMetadata


class TeardownHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        key: Optional[str]=None,
        metadata: Dict[str, Any]={}
    ) -> None:

        if metadata is None:
            metadata = {}

        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.TEARDOWN
        )

        self.key = key
        self.metadata = HookMetadata(**metadata)

    async def call(self, **kwargs):
    
        context: SimpleContext = kwargs.get('context')

        results = await self._call(**{name: value for name, value in kwargs.items() if name in self.params})

        if self.key:
            context[self.key] = results

        if isinstance(results, dict):
            return {
                **kwargs,
                **results
            }

        return {
            **kwargs,
            'context': results
        }

    def copy(self):
        teardown_hook = TeardownHook(
            self.name,
            self.shortname,
            self._call,
            key=self.key,
            metadata={
                'weight': self.metadata.weight,
                'order': self.metadata.order,
                'env': self.metadata.env,
                'user': self.metadata.user,
                'tags': self.metadata.tags
            }
        )

        teardown_hook.stage = self.stage

        return teardown_hook