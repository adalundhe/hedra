
import asyncio
import time
import inspect
from typing import Dict, Generic, TypeVar
from hedra.plugins.types.common.plugin_hook import PluginHook
from hedra.plugins.types.common.types import PluginHooks
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.common.registrar import plugin_registrar
from hedra.core.personas.types.default_persona.default_persona import (
    DefaultPersona,
    cancel_pending
)
from hedra.core.engines.client.config import Config


T = TypeVar('T')


class PersonaPlugin(DefaultPersona, Generic[T], Plugin):
    type=PluginType.PERSONA
    
    def __init__(self, config: Config):
        super().__init__(config)

        self.hooks: Dict[PluginHooks, PluginHook] = {}
        self.config = config

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

                method_name = method.__qualname__
                hook: PluginHook = plugin_registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(self, self.__class__)
                    setattr(self, hook.shortname, hook.call)

                    self.hooks[hook.hook_type] = hook

    async def execute(self):

        setup_hook = self.hooks.get(PluginHooks.ON_PERSONA_SETUP)

        if setup_hook:
            await setup_hook.call()

        hooks = self._hooks
        loop = asyncio.get_running_loop()

        generate_hook = self.hooks.get(PluginHooks.ON_PERSONA_GENERATE)
        shutdown_hook = self.hooks.get(PluginHooks.ON_PERSONA_SHUTDOWN)

        await self.start_updates()

        self.start = time.monotonic()
        completed, pending = await asyncio.wait([
            loop.create_task(
                hooks[action_idx].session.execute_prepared_request(
                    hooks[action_idx].action
                )
            ) async for action_idx in generate_hook.call()
        ], timeout=self.graceful_stop)

        self.end = time.monotonic()
        self.pending_actions = len(pending)

        results = await asyncio.gather(*completed)
        
        await self.stop_updates()

        await asyncio.gather(*[
            asyncio.create_task(
                cancel_pending(pend)
            ) for pend in pending
        ])

        for hook in hooks:
            await hook.session.close()

        if shutdown_hook:
            await shutdown_hook.call()
        
        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return results
