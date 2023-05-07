import inspect
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.common.plugin_hook import PluginHook
from hedra.plugins.types.common.types import PluginHooks
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.registrar import plugin_registrar
from typing import Dict


class ExtensionPlugin(Plugin):
    type=PluginType.EXTENSION

    def __init__(self) -> None:
        super(
            ExtensionPlugin
        ).__init__(self)

        self.hooks: Dict[PluginHooks, PluginHook] = {}

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

                method_name = method.__qualname__
                hook: PluginHook = plugin_registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(self, self.__class__)
                    setattr(self, hook.shortname, hook.call)

                    self.hooks[hook.hook_type] = hook

    async def execute(self) -> None:
        execute_hook = self.hooks.get(PluginHooks.ON_EXTENSION_EXECUTE)
        await execute_hook.call()