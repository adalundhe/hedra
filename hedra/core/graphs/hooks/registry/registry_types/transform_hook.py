import asyncio
from typing import Dict, Coroutine, List
from typing import Callable, Awaitable, Any, Optional
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.engines.types.common.results_set import ResultsSet
from .hook import Hook


class TransformHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        timeout: Optional[float]='1m',
        *names: str,
        load: Optional[str]=None,
        store: Optional[str]=None,
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
        self.load = load
        self.store = store
        self.events: Dict[str, Coroutine] = {}
        self.order = order
        self.context: Optional[SimpleContext] = None
        self.conditions: Optional[List[Callable[..., bool]]] = []

        if self.store and self.load is None:
            self.load = self.store

    @property
    def key(self):
        if self.load:
            return self.load

        if self.store:
            return self.store

        return None

    async def call(self, value: Any):

        if self.load == 'results':
            results = []
            context_data: Dict[str, ResultsSet] = value
            for stage in context_data.values():
                results.extend(stage.results)

            value = results

    
        if isinstance(value, list) is False and (await self._execute_call() is True):
            return await asyncio.wait_for(
                self._call(value),
                timeout=self.timeout
            )

        else:

            result = await asyncio.wait_for(
                asyncio.gather(*[
                    asyncio.create_task(
                        
                        self._call(data_item)
                    ) for data_item in value if (
                        await self._execute_call(data_item) is True
                    )
                ]),
                timeout=self.timeout
            )

            return [
                data_item for data_item in result if data_item is not None
            ]

    async def _execute_call(self, value: Any):
        execute = True
        for condition in self.conditions:
            execute = await condition(value)

        return execute
