import functools
from typing import Dict, List, Union, Tuple
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.ACTION)
def action(
    *names: Tuple[str, ...],
    weight: int=1, 
    order: int=1, 
    metadata: Dict[str, Union[str, int]]={}, 
    checks: List[str]=[], 
    notify: List[str]=[], 
    listen: List[str]=[]
):

    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):

            return func(*args, **kwargs)
                
        return decorator

    return wrapper