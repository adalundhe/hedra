from typing import Callable, Awaitable, Any, Optional
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType


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
        metric_hook = MetricHook(
            self.name,
            self.shortname,
            self._call,
            self.group,
            order=self.order
        )

        metric_hook.stage = self.stage

        return metric_hook