from collections import defaultdict
from types import FunctionType
from typing import Any, Dict, List, Union, Coroutine
from hedra.core.graphs.hooks.types.hook import Hook, Metadata
from hedra.core.graphs.hooks.types.hook_types import HookType


class Registrar:
    all: Dict[str, Hook] = {}
    reserved: Dict[str, Dict[str, Hook]] = defaultdict(dict)
    module_paths: Dict[str, str] = {}

    def __init__(self, hook_type) -> None:

        self.hook_type = hook_type

    def __call__(self, hook: FunctionType) -> Any:
        self.module_paths[hook.__name__] = hook.__module__
        return self.add_hook(self.hook_type)

    def add_hook(self, hook_type: str):
        if hook_type == HookType.SETUP or hook_type == HookType.TEARDOWN:

            def wrap_hook(metadata: Dict[str, Union[str, int]]={}):

                def wrapped_method(func):

                    hook_name = func.__qualname__
                    hook_shortname = func.__name__


                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        hook_type=hook_type,
                        metadata=Metadata(
                            **metadata
                        )
                    )

                    return func
                
                return wrapped_method

        elif hook_type in [HookType.METRIC]:
            def wrap_hook(group: str='user_metrics'):
                def wrapped_method(func):

                    hook_name = func.__qualname__
                    hook_shortname = func.__name__


                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        hook_type=hook_type,
                        metadata=Metadata(),
                        group=group
                    )

                    return func
                
                return wrapped_method

        elif hook_type in [HookType.BEFORE, HookType.AFTER, HookType.CHECK]:
            
            def wrap_hook(*names):
                def wrapped_method(func):
                    hook_name = func.__qualname__
                    hook_shortname = func.__name__

                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        hook_type=hook_type,
                        names=names
                    )

                    return func
                
                return wrapped_method

        elif hook_type == HookType.VALIDATE:

            def wrap_hook(stage: str, *names):
                def wrapped_method(func):
                    hook_name = func.__qualname__
                    hook_shortname = func.__name__

                    target_hook_names = [f'{stage}.{name}' for name in names]

                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        stage=stage,
                        hook_type=hook_type,
                        names=target_hook_names
                    )

                    return func
                
                return wrapped_method

        elif hook_type == HookType.CHANNEL:

            def wrap_hook():
                def wrapped_method(func):
                    hook_name = func.__qualname__
                    hook_shortname = func.__name__

                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        hook_type=hook_type
                    )

                    return func
                    
                return wrapped_method

        elif hook_type == HookType.SAVE:

            def wrap_hook(checkpoint_filepath: str):
                def wrapped_method(func):
                    hook_name = func.__qualname__
                    hook_shortname = func.__name__

                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        hook_type=hook_type,
                        metadata=Metadata(path=checkpoint_filepath)
                    )

                    return func
                
                return wrapped_method

        else:

            def wrap_hook(
                weight: int=1, 
                order: int=1, 
                metadata: Dict[str, Union[str, int]]={}, 
                checks: List[Coroutine]=[],
                notify: List[str]=[],
                listen: List[str]=[]
            ):
                def wrapped_method(func):

                    hook_name = func.__qualname__
                    hook_shortname = func.__name__

                    self.all[hook_name] = Hook(
                        hook_name, 
                        hook_shortname,
                        func, 
                        hook_type=hook_type,
                        notify=notify,
                        listen=listen,
                        metadata=Metadata(
                            weight=weight,
                            order=order,
                            **metadata
                        ),
                        checks=checks
                    )

                    return func

                return wrapped_method

        return wrap_hook
        

def makeRegistrar():

    return Registrar


registrar = makeRegistrar()