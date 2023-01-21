import inspect
import functools
from collections import defaultdict
from types import FunctionType
from typing import Any, Dict
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.validation_types.action import ActionValidator
from hedra.core.graphs.hooks.validation_types.after import AfterValidator
from hedra.core.graphs.hooks.validation_types.before import BeforeValidator
from hedra.core.graphs.hooks.validation_types.check import CheckValidator
from hedra.core.graphs.hooks.validation_types.context import ContextValidator
from hedra.core.graphs.hooks.validation_types.event import EventValidator
from hedra.core.graphs.hooks.validation_types.metric import MetricValidator
from hedra.core.graphs.hooks.validation_types.restore import RestoreValidator
from hedra.core.graphs.hooks.validation_types.save import SaveValidator
from hedra.core.graphs.hooks.validation_types.setup import SetupValidator
from hedra.core.graphs.hooks.validation_types.task import TaskValidator
from hedra.core.graphs.hooks.validation_types.teardown import TeardownValidator
from hedra.core.graphs.hooks.validation_types.validate import ValidateValidator
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

        self.validator_types = {
            HookType.ACTION: ActionValidator,
            HookType.AFTER: lambda *args, **kwargs: AfterValidator(*args, **kwargs),
            HookType.BEFORE: lambda *args, **kwargs: BeforeValidator(*args, **kwargs),
            HookType.CHECK: lambda *args, **kwargs: CheckValidator(*args, **kwargs),
            HookType.CONTEXT: ContextValidator,
            HookType.EVENT: lambda *args, **kwargs: EventValidator(*args, **kwargs),
            HookType.METRIC: lambda *args, **kwargs: MetricValidator(*args, **kwargs),
            HookType.RESTORE: lambda *args, **kwargs: RestoreValidator(*args, **kwargs),
            HookType.SAVE: lambda *args, **kwargs: SaveValidator(*args, **kwargs),
            HookType.SETUP: lambda *args, **kwargs: SetupValidator(*args, **kwargs),
            HookType.TASK: lambda *args, **kwargs: TaskValidator(*args, **kwargs),
            HookType.TEARDOWN: lambda *args, **kwargs: TeardownValidator(*args, **kwargs),
            HookType.VALIDATE: lambda *args, **kwargs: ValidateValidator(*args, **kwargs),
        }


    def __call__(self, hook: FunctionType):

  
        self.module_paths[hook.__name__] = hook.__module__

        @functools.wraps(hook)
        def wrap_hook(*args, **kwargs):
        
            def wrapped_method(func):

                hook_name = func.__qualname__
                hook_shortname = func.__name__

                hook = self.hook_types[self.hook_type]

                hook_args = args
                args_count = len(args)
                
                if args_count < 1:
                    hook_args = []

                validator = self.validator_types.get(self.hook_type)

                if validator:
                    try:
                        validator(*hook_args, **kwargs)

                    except TypeError as e:
                        raise e
                        

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