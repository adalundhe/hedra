import asyncio
from collections import defaultdict
from typing import (
    Dict, 
    Coroutine, 
    List, 
    Tuple, 
    Optional, 
    Callable, 
    Awaitable, 
    Any
)
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.simple_context import SimpleContext


class TransformHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Optional[Tuple[str, ...]],
        timeout: Optional[float]='1m',
        pre: bool=False,
        order: int=1,
        skip: bool=False
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            order=order,
            skip=skip,
            hook_type=HookType.TRANSFORM
        )
        
        parser = TimeParser(time_amount=timeout)
        
        self.timeout = parser.time
        self.names = list(set(names))
        self.pre = pre
        self.events: Dict[str, Coroutine] = {}
        self.context: Optional[SimpleContext] = None
        self.conditions: Optional[List[Callable[..., bool]]] = []
        self._timeout_as_string = timeout
        
    async def call(self, **kwargs):

        if self.skip:
            return kwargs
        
        batchable_args: List[Dict[str, Any]] = []
        for name, arg in kwargs.items():
            if isinstance(arg, (list, tuple)):
                batchable_args.extend([
                    {**kwargs, name: item} for item in arg
                ])

        if len(batchable_args) > 0:
 
            result = await asyncio.wait_for(
                asyncio.gather(*[
                    asyncio.create_task(         
                        self._call(**{name: value for name, value in call_kwargs.items() if name in self.params})
                    ) for call_kwargs in batchable_args if (
                        await self._execute_call(**call_kwargs) is True
                    )
                ]),
                timeout=self.timeout
            )

            aggregated_transformm = defaultdict(list)

            for data_item in result:
                if isinstance(data_item, dict):
                    for name, value in data_item.items():
                        if isinstance(value, (list, tuple)):
                            aggregated_transformm[name].extend(value)

                        else:
                            aggregated_transformm[name].append(value)

                elif data_item is not None:
                    aggregated_transformm[self.shortname].append(data_item)


            return {
                **kwargs,
                **dict(aggregated_transformm)
            }

        else:     
    
            result = await self._call(**{name: value for name, value in kwargs.items() if name in self.params})

            if isinstance(result, dict):
                return {
                    **kwargs,
                    **result
                }

            return {
                **kwargs,
                self.shortname: result
            }

    def copy(self):
        transform_hook = TransformHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            timeout=self._timeout_as_string,
            pre=self.pre,
            order=self.order,
            skip=self.skip,
        )
        
        transform_hook.stage = self.stage

        return transform_hook