from typing import Coroutine
from .types import PluginHooks


class PluginHook:

    def __init__(
        self, 
        name: str, 
        shortname: str,
        call: Coroutine, 
        plugin: str = None,
        hook_type=PluginHooks.ON_EXECUTE,
    ) -> None:
        self.name = name
        self.shortname = shortname
        self.call = call
        self.plugin = plugin
        self.hook_type = hook_type