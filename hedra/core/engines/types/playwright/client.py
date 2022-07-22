import asyncio
import random
from async_tools.datatypes import AsyncList
from types import FunctionType
from typing import Awaitable, Dict, List, Optional, Set, Tuple, Union
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.common.types import RequestTypes
from .context_config import ContextConfig
from .context import Context
from .pool import ContextPool
from .command import Command
from .result import Result


PlaywrightResponseFuture = Awaitable[Union[Result, Exception]]
PlaywrightBatchResponseFuture = Awaitable[Tuple[Set[PlaywrightResponseFuture], Set[PlaywrightResponseFuture]]]


class MercuryPlaywrightClient:

    def __init__(self,  concurrency: int = 500, group_size: int=50, timeouts: Timeouts = Timeouts()) -> None:
        self.concurrency = concurrency
        self.pool = ContextPool(concurrency, group_size)
        self.timeouts = timeouts
        self.registered: Dict[str, Command] = {}
        self.sem = asyncio.Semaphore(self.concurrency)
        self.loop = asyncio.get_event_loop()
        self.context = Context()

    async def setup(self, config: ContextConfig):
        self.pool.create_pool(config)
        for context_group in self.pool:
            await context_group.create()

    async def prepare(self, command: Command) -> Awaitable[None]:

        command.options.extra = {
            **command.options.extra,
            'timeout': self.timeouts.total_timeout * 1000
        }

        self.registered[command.name] = command

    async def execute_prepared_command(self, command: Command) -> PlaywrightResponseFuture:

        result = Result(command, type=RequestTypes.PLAYWRIGHT)
        
        async with self.sem:
            context = self.pool.contexts.pop()
            try:
                if command.hooks.before:
                    command = await command.hooks.before(command)

                result = await context.execute(command)

                self.context.last[command.name] = result

                if command.hooks.after:
                    response = await command.hooks.after(response)

                self.pool.contexts.append(context)

                return result

            except Exception as e:
                result.error = e
                self.context.last[command.name] = response
                self.pool.contexts.append(context)

                return result
                
    async def request(self, command: Command, checks: Optional[List[FunctionType]]=[]) -> PlaywrightResponseFuture:

        if self.registered.get(command.name) is None:
            await self.prepare(command, checks)

        return await self.execute_prepared_command(command)

    def execute_batch(
        self, 
        command: Command,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None,
        checks: Optional[List[FunctionType]]=[]
    ) -> PlaywrightBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        return [ asyncio.create_task(
            self.execute_prepared_command(command)
        ) for _ in range(concurrency)]

    async def close(self):
        for context_group in self.pool:
            await context_group.close()