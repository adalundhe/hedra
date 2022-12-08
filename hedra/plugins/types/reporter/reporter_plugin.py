import asyncio
from concurrent.futures import ThreadPoolExecutor
import inspect
from typing import Dict, Generic, TypeVar

import psutil
from hedra.plugins.types.common.plugin_hook import PluginHook
from hedra.plugins.types.common.types import PluginHooks
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.common.registrar import plugin_registrar


T = TypeVar('T')


class ReporterPlugin(Generic[T], Plugin):
    type=PluginType.REPORTER
    config: T = None

    def __init__(self, config: T) -> None:
        
        super(
            ReporterPlugin,
            self
        ).__init__()

        self.loop = asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self.hooks: Dict[PluginHooks, PluginHook] = {}
        self.name: str = None
    
        self.config = config

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

                method_name = method.__qualname__
                hook: PluginHook = plugin_registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(self, self.__class__)
                    setattr(self, hook.shortname, hook.call)

                    self.hooks[hook.hook_type] = hook

        connect_hook = self.hooks.get(PluginHooks.ON_REPORTER_CONNECT)
        close_hook = self.hooks.get(PluginHooks.ON_REPORTER_CLOSE)
        
        submit_events_hook = self.hooks.get(PluginHooks.ON_PROCESS_EVENTS)
        submit_shared_stats_hook = self.hooks.get(PluginHooks.ON_PROCESS_SHARED_STATS)
        submit_metrics_hook = self.hooks.get(PluginHooks.ON_PROCESS_METRICS)
        submit_custom_stats_hook = self.hooks.get(PluginHooks.ON_PROCESS_CUSTOM_STATS)
        submit_errors_hook = self.hooks.get(PluginHooks.ON_PROCESS_ERRORS)
        
        self.connect = connect_hook.call
        self.close = close_hook.call

        self.submit_events = submit_events_hook.call
        self.submit_common = submit_shared_stats_hook.call
        self.submit_metrics = submit_metrics_hook.call
        self.submit_custom = submit_custom_stats_hook.call
        self.submit_errors = submit_errors_hook.call
