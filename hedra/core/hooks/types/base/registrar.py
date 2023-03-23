import functools
from collections import defaultdict
from types import FunctionType
from typing import Dict, List
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.channel.hook import ChannelHook
from hedra.core.hooks.types.check.hook import CheckHook
from hedra.core.hooks.types.condition.hook import ConditionHook
from hedra.core.hooks.types.context.hook import ContextHook
from hedra.core.hooks.types.event.hook import EventHook
from hedra.core.hooks.types.load.hook import LoadHook
from hedra.core.hooks.types.metric.hook import MetricHook
from hedra.core.hooks.types.save.hook import SaveHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.hooks.types.transform.hook import TransformHook
from .hook import Hook
from .hook_type import HookType



class Registrar:
    all: Dict[str, List[Hook]] = {}
    reserved: Dict[str, Dict[str, Hook]] = defaultdict(dict)
    module_paths: Dict[str, str] = {}

    def __init__(self, hook_type) -> None:
        self.hook_type = hook_type
        self.hook_types = {
            HookType.ACTION: lambda *args, **kwargs: ActionHook(*args, **kwargs),
            HookType.CHANNEL: lambda *args, **kwargs:  ChannelHook(*args, **kwargs),
            HookType.CHECK: lambda *args, **kwargs:  CheckHook(*args, **kwargs),
            HookType.CONDITION: lambda *args, **kwargs: ConditionHook(*args, **kwargs),
            HookType.CONTEXT: lambda *args, **kwargs:  ContextHook(*args, **kwargs),
            HookType.EVENT: lambda *args, **kwargs:  EventHook(*args, **kwargs),
            HookType.METRIC: lambda *args, **kwargs:  MetricHook(*args, **kwargs),
            HookType.LOAD: lambda *args, **kwargs:  LoadHook(*args, **kwargs),
            HookType.SAVE: lambda *args, **kwargs:  SaveHook(*args, **kwargs),
            HookType.TASK: lambda *args, **kwargs:  TaskHook(*args, **kwargs),
            HookType.TRANSFORM: lambda *args, **kwargs: TransformHook(*args, **kwargs),
        }

    def __call__(self, hook):
        self.module_paths[hook.__name__] = hook.__module__

  
        def wrap_hook(*args, **kwargs):

            def wrapped_method(func):

                hook_name = func.__qualname__
                hook_shortname = func.__name__

                hook = self.hook_types[self.hook_type]

                hook_args = args
                args_count = len(args)
                
                if args_count < 1:
                    hook_args = []
                        
                if hook_name not in self.all:
                    self.all[hook_name] = [
                        hook(
                            hook_name,
                            hook_shortname,
                            func,
                            *hook_args,
                            **kwargs
                        )
                    ]

                else:
                    self.all[hook_name].append(hook(
                        hook_name,
                        hook_shortname,
                        func,
                        *hook_args,
                        **kwargs
                    ))

                return func
            
            return wrapped_method

        return wrap_hook
        

def makeRegistrar():
    return Registrar


registrar = makeRegistrar()