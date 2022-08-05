import asyncio
from typing import Awaitable, Dict, Set, Tuple, Union

from django import conf
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.common.types import RequestTypes
from .context_config import ContextConfig
from .pool import ContextPool
from .command import Command
from .result import PlaywrightResult


PlaywrightResponseFuture = Awaitable[Union[PlaywrightResult, Exception]]
PlaywrightBatchResponseFuture = Awaitable[Tuple[Set[PlaywrightResponseFuture], Set[PlaywrightResponseFuture]]]


class MercuryPlaywrightClient:

    def __init__(self,  concurrency: int = 500, group_size: int=50, timeouts: Timeouts = Timeouts()) -> None:
        self.pool = ContextPool(concurrency, group_size)
        self.timeouts = timeouts
        self.registered: Dict[str, Command] = {}
        self.sem = asyncio.Semaphore(concurrency)
        self.config = None

    async def setup(self, config: ContextConfig):
        self.config = config
        self.pool.create_pool(self.config)
        for context_group in self.pool:
            await context_group.create()

    async def prepare(self, command: Command) -> Awaitable[None]:

        command.options.extra = {
            **command.options.extra,
            'timeout': self.timeouts.total_timeout * 1000
        }

        self.registered[command.name] = command

    async def execute_prepared_command(self, command: Command) -> PlaywrightResponseFuture:

        result = PlaywrightResult(command, type=RequestTypes.PLAYWRIGHT)
        
        async with self.sem:
            context = self.pool.contexts.pop()
            try:
                if command.hooks.before:
                    command = await command.hooks.before(command, result)

                result = await context.execute(command)

                if command.hooks.after:
                    command = await command.hooks.after(command, result)

                self.pool.contexts.append(context)

                return result

            except Exception as e:
                result.error = e
                self.pool.contexts.append(context)

                return result
                