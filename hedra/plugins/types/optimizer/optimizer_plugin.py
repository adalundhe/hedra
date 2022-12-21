from ctypes import Union
import inspect
import math
from typing import Any, Dict
from hedra.core.graphs.stages.optimize.optimization.algorithms.types.base_algorithm import BaseAlgorithm
from hedra.plugins.types.common.plugin_hook import PluginHook
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.types import PluginHooks
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.common.registrar import plugin_registrar


class OptimizerPlugin(BaseAlgorithm, Plugin):
    type=PluginType.OPTIMIZER

    def __init__(self, config: Dict[str, Any]) -> None:
        self.hooks: Dict[PluginHooks, PluginHook] = {}
        self.name: str = None
        
        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

                method_name = method.__qualname__
                hook: PluginHook = plugin_registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(self, self.__class__)
                    setattr(self, hook.shortname, hook.call)

                    self.hooks[hook.hook_type] = hook


        on_get_params = self.hooks.get(PluginHooks.ON_OPTIMIZER_GET_PARAMS)
        on_update_params = self.hooks.get(PluginHooks.ON_OPTIMIZER_UPDATE_PARAMS)
        on_optimize = self.hooks.get(PluginHooks.ON_OPTIMIZE)

        if on_get_params:
            self.get_params = on_get_params.call

        if on_update_params:
            self.update_params = on_update_params.call

        self.optimize = on_optimize.call

        super().__init__(config)




        
        
