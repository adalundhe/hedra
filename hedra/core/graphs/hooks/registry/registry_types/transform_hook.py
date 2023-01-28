import asyncio
from collections import defaultdict
from typing import Dict, Coroutine, List, Tuple, Optional
from typing import Callable, Awaitable, Any, Optional
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.simple_context import SimpleContext
from .hook import Hook


class TransformHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Optional[Tuple[str, ...]],
        timeout: Optional[float]='1m',
        pre: bool=False,
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.TRANSFORM
        )
        parser = TimeParser(time_amount=timeout)
        self.timeout = parser.time
        self.names = list(set(names))
        self.pre = pre
        self.events: Dict[str, Coroutine] = {}
        self.order = order
        self.context: Optional[SimpleContext] = None
        self.conditions: Optional[List[Callable[..., bool]]] = []
        
    async def call(self, **kwargs):
        batchable_args: List[Dict[str, Any]] = []
        for name, arg in kwargs.items():
            if isinstance(arg, (list, tuple)):
                batchable_args.extend([
                    {**kwargs, name: item} for item in arg
                ])
            
        if len(batchable_args) > 0:
            execute = await self._execute_call(*batchable_args)

            if execute:
                result = await asyncio.wait_for(
                    asyncio.gather(*[
                        asyncio.create_task(         
                            self._call(**call_kwargs)
                        ) for call_kwargs in batchable_args if (
                            await self._execute_call(arg) is True
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
                                aggregated_transformm[name] = value

                    else:
                        aggregated_transformm['transformed'].append(data_item)


                return {
                    **kwargs,
                    **dict(aggregated_transformm)
                }

            return kwargs

        else:     
            
            execute = await self._execute_call(**kwargs)
            if execute:

                result = await self._call(**kwargs)

                if isinstance(result, dict):
                    return {
                        **kwargs,
                        **result
                    }

                return {
                    **kwargs,
                    'transformed': result
                }
