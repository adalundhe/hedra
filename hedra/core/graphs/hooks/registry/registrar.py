from collections import defaultdict
from types import FunctionType
from typing import Any, Dict
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .registry_types import (
    ActionHook,
    AfterHook,
    BeforeHook,
    ChannelHook,
    CheckHook,
    ContextHook,
    EventHook,
    MetricHook,
    RestoreHook,
    SaveHook,
    SetupHook,
    TaskHook,
    TeardownHook,
    ValidateHook
)




class Registrar:
    all: Dict[str, Hook] = {}
    reserved: Dict[str, Dict[str, Hook]] = defaultdict(dict)
    module_paths: Dict[str, str] = {}

    def __init__(self, hook_type) -> None:
        self.hook_type = hook_type
        self.hook_types = {
            HookType.ACTION: lambda *args, **kwargs: ActionHook(*args, **kwargs),
            HookType.AFTER: lambda *args, **kwargs:  AfterHook(*args, **kwargs),
            HookType.BEFORE: lambda *args, **kwargs:  BeforeHook(*args, **kwargs),
            HookType.CHANNEL: lambda *args, **kwargs:  ChannelHook(*args, **kwargs),
            HookType.CHECK: lambda *args, **kwargs:  CheckHook(*args, **kwargs),
            HookType.CONTEXT: lambda *args, **kwargs:  ContextHook(*args, **kwargs),
            HookType.EVENT: lambda *args, **kwargs:  EventHook(*args, **kwargs),
            HookType.METRIC: lambda *args, **kwargs:  MetricHook(*args, **kwargs),
            HookType.RESTORE: lambda *args, **kwargs:  RestoreHook(*args, **kwargs),
            HookType.SAVE: lambda *args, **kwargs:  SaveHook(*args, **kwargs),
            HookType.SETUP: lambda *args, **kwargs:  SetupHook(*args, **kwargs),
            HookType.TASK: lambda *args, **kwargs:  TaskHook(*args, **kwargs),
            HookType.TEARDOWN: lambda *args, **kwargs:  TeardownHook(*args, **kwargs),
            HookType.VALIDATE: lambda *args, **kwargs:  ValidateHook(*args, **kwargs)
        }

    def __call__(self, hook: FunctionType) -> Any:
        self.module_paths[hook.__name__] = hook.__module__
        return self.add_hook(self.hook_type)

    def add_hook(self, hook_type: str):

        def wrap_hook(*args, **kwargs):

            def wrapped_method(func):

                hook_name = func.__qualname__
                hook_shortname = func.__name__

                hook = self.hook_types[hook_type]

                hook_args = args
                args_count = len(args)
                
                if args_count < 1:
                    hook_args = []

                self.all[hook_name] = hook(
                    hook_name,
                    hook_shortname,
                    func,
                    *hook_args,
                    **kwargs
                )

                return func
            
            return wrapped_method

        return wrap_hook
        

def makeRegistrar():
    return Registrar


registrar = makeRegistrar()