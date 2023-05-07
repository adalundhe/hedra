import inspect
from inspect import Parameter
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.common.plugin_hook import PluginHook
from hedra.plugins.types.common.types import PluginHooks
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.registrar import plugin_registrar
from typing import (
    Dict, 
    Any, 
    Union, 
    Mapping
)
from .types import ExtensionType


class ExtensionPlugin(Plugin):
    type=PluginType.EXTENSION
    name: str=None
    
    def __init__(self) -> None:
        super(
            ExtensionPlugin
        ).__init__(self)

        self.hooks: Dict[PluginHooks, PluginHook] = {}
        self.name = self.name
        self._args: Dict[str, Mapping[str, Parameter]] = {}

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

                method_name = method.__qualname__
                hook: PluginHook = plugin_registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(self, self.__class__)
                    setattr(self, hook.shortname, hook.call)

                    self.hooks[hook.hook_type] = hook
                    args = inspect.signature(hook.call)

                    self._args[hook.hook_type] = args.parameters

        self.extension_type: ExtensionType = None

    async def execute(
        self,
        **kwargs
    ) -> Dict[str, Any]:
        next_args = {
             **kwargs
        }

        execute_hook = self.hooks.get(PluginHooks.ON_EXTENSION_PREPARE)
        hook_args = self._args.get(PluginHooks.ON_EXTENSION_PREPARE)

        result: Union[Dict[str, Any], Any] = await execute_hook.call(
            **{
                 name: value for name, value in next_args.items() if name in hook_args
            }
        )

        if isinstance(result, dict) is False:
             result = {
                  'extension_data': result
             }

        next_args = {
             **kwargs,
             **result
        }

        prepare_hook = self.hooks.get(PluginHooks.ON_EXTENSION_EXECUTE)
        hook_args = self._args.get(PluginHooks.ON_EXTENSION_EXECUTE)
        
        return await prepare_hook.call(
             **{
                 name: value for name, value in next_args.items() if name in hook_args
            }
        )