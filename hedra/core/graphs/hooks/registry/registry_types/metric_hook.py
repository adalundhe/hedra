from typing import Type, Callable, Awaitable, Any, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class MetricHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        group: Optional[str] = None,
        order: int=1
    ) -> None:        
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.METRIC
        )

        self.group = group
        self.order = order

    async def call(self, **kwargs):
        metric = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(metric, dict):
            return {
                **kwargs,
                **metric
            }

        return {
            **kwargs,
            'metric': metric
        }

    def copy(self):
        return MetricHook(
            self.name,
            self.shortname,
            self._call,
            self.group,
            order=self.order
        )