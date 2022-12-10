
from types import FunctionType
from typing import Any, Dict
from .plugin_hook import PluginHook


class PluginRegistrar:

    all: Dict[str, PluginHook] = {}
    module_paths: Dict[str, str] = {}

    def __init__(self, hook_type) -> None:
        self.hook_type = hook_type

    def __call__(self, plugin_hook: FunctionType) -> Any:
        self.module_paths[plugin_hook.__name__] = plugin_hook.__module__
        return self.add_hook(self.hook_type)

    def add_hook(self, hook_type: str):
        def wrap_hook():
            def wrapped_method(func):

                hook_name = func.__qualname__
                hook_shortname = func.__name__

                self.all[hook_name] = PluginHook(
                    hook_name,
                    hook_shortname,
                    func,
                    hook_type=hook_type
                )

                return func
            
            return wrapped_method

        return wrap_hook


def makePluginRegistrar():

    return PluginRegistrar


plugin_registrar = makePluginRegistrar()