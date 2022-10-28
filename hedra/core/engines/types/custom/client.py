import asyncio
import time
from ctypes import Union
import traceback
from typing import Any, Awaitable, Dict, Generic, TypeVar, Union
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.concurrency import Semaphore
from hedra.plugins.types.engine.action import Action
from hedra.plugins.types.engine.result import Result
from hedra.plugins.types.engine.hooks.types import PluginHooks
from .connection import CustomConnection
from .pool import CustomPool

A = TypeVar('A')
R = TypeVar('R')

class MercuryCustomClient(Generic[A, R]):

    def __init__(
        self, 
        plugin: Any,
        concurrency: int=10**3, 
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool=False
    ) -> None:
        self.timeouts = timeouts

        self.registered: Dict[str, CustomConnection] = {}
        self._hosts = {}
        self.closed = False

        self.sem = asyncio.Semaphore(value=concurrency)
        
        self.active = 0
        self.waiter: asyncio.Future = None
        self.plugin: Any = plugin

        self._on_connect = self.plugin.hooks.get(PluginHooks.ON_CONNECT)
        self._on_execute = self.plugin.hooks.get(PluginHooks.ON_EXECUTE)
        self._on_close = self.plugin.hooks.get(PluginHooks.ON_CLOSE)

        self.custom_connection: CustomConnection = lambda reset_connection: CustomConnection(
            security_context=self.plugin.security_context,
            reset_connection=reset_connection,
            on_connect=self._on_connect.call,
            on_execute=self._on_execute.call,
            on_close=self._on_close.call
        )

        self.pool = CustomPool(
            self.custom_connection,
            concurrency, 
            reset_connections=reset_connections
        )

        self.pool.create_pool()


    def extend_pool(self, increased_capacity: int):
        self.pool.size += increased_capacity
        for _ in range(increased_capacity):
            self.pool.connections.append(
                self.custom_connection(self.pool.reset_connections)
            )
        
        self.sem = Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity
        self.pool.connections = self.pool.connections[:self.pool.size]
        self.sem = Semaphore(self.pool.size)

    async def wait_for_active_threshold(self):
        if self.waiter is None:
            self.waiter = asyncio.get_event_loop().create_future()
            await self.waiter

    async def prepare(self, action: Action) -> Awaitable[Union[CustomConnection, Exception]]:
        try:
            connection: CustomConnection = self.custom_connection(self.pool.reset_connections)

            if action.use_security_context:
                action.security_context = connection.security_context

            await asyncio.wait_for(
                connection.make_connection(action),
                timeout=self.timeouts.connect_timeout
            )

            if action.is_setup is False:
                action.setup()

            self.registered[action.name] = action

        except Exception as e:       
            raise e

    async def execute_prepared_request(self, action: Action) -> Awaitable[Result[R]]:
 
        result: Result = self.plugin.result(action)
        
        result.times['wait_start'] = time.monotonic()
        self.active += 1
 
        async with self.sem:
            connection = self.pool.connections.pop()
            
            try:
                if action.hooks.before:
                    action = await action.hooks.before(action, result)
                    action.setup()

                result.times['start'] = time.monotonic()

                result = await asyncio.wait_for(
                    connection.execute(action, result),
                    timeout=self.timeouts.total_timeout
                )

                result.times['complete'] = time.monotonic()
       
                self.pool.connections.append(connection)

                if action.hooks.after:
                    action = await action.hooks.after(action, result)
                    action.setup()

            except Exception as e:
                result.times['complete'] = time.monotonic()
                result.error = str(e)

                self.pool.connections.append(
                    self.custom_connection(self.pool.reset_connections)
                )

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            return result