from __future__ import annotations
import asyncio
from ctypes import Union
import inspect
from typing import Any, Awaitable, Generic, Optional, TypeVar
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.custom.client import MercuryCustomClient as CustomSession
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.event import Event
from hedra.plugins.types.common.plugin_hook import PluginHook
from hedra.plugins.types.common.types import PluginHooks
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.common.registrar import plugin_registrar
from .action import Action
from .result import Result


A = TypeVar('A')
R = TypeVar('R')


class EnginePlugin(Generic[A, R], Plugin):
    action: Action[A] = None
    result: Result[R] = None
    event: Event[Result[R]] = Event
    security_context: Any = None
    initialized: bool = False
    type=PluginType.ENGINE

    def __init__(self, config: Config) -> None:

        super(
            EnginePlugin,
            self
        ).__init__()

        self.hooks = {}
        self.name: str = None
        self.request_type = self.__class__.__name__
        self.next_name = None
        self.intercept = False
        self.waiter = None
        self.actions: ActionsStore = None
        self.registered = {}
        self.metadata_string: str = None

        self.config = config
        

        self.action_type = A
        self.request_type = R

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

                method_name = method.__qualname__
                hook: PluginHook = plugin_registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(self, self.__class__)
                    setattr(self, hook.shortname, hook.call)

                    self.hooks[hook.hook_type] = hook

 
        self.session = CustomSession[A, R](
            self,
            concurrency=config.batch_size,
            timeouts=Timeouts(
                connect_timeout=config.connect_timeout,
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections
        )

    def __getattr__(self, attribute_name: str):

        session = object.__getattribute__(self, 'session')

        if hasattr(session, attribute_name):
            return getattr(session, attribute_name)
            
        else:
            # Default behaviour
            return object.__getattribute__(self, attribute_name)

    async def execute(self, *args, **kwargs) -> Awaitable[Result[R]]:
        
        if self.registered.get(self.next_name) is None:
            action: Action[A] = self.action(
                self.next_name,
                *args,
                **kwargs
            )

            action.plugin_type = self.name
            
            await self.session.prepare(action)

            if self.intercept:
                self.actions.store(self.next_name, action, self)
                
                loop = asyncio.get_event_loop()
                self.waiter = loop.create_future()
                await self.waiter

        return await self.session.execute_prepared_request(
            self.session.registered.get(self.next_name)
        )

    async def close(self):
        close_hook = self.hooks.get(PluginHooks.ON_ENGINE_CLOSE)
        await close_hook.call()


    
