import inspect
from collections import defaultdict
from types import MethodType
from typing import List, Dict
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.stages.validate.exceptions import (
    HookValidationError,
    ReservedMethodError
)
from hedra.logging import HedraLogger


class StageValidator:

    def __init__(self, stage: Stage, metadata_string: str) -> None:

        self.logger = HedraLogger()
        self.logger.initialize()
        self.metadata_string: str = metadata_string
        self.stage: Stage = stage
        self.hooks_by_name: Dict[str, Hook] = defaultdict(dict)
        self.hooks: Dict[HookType, List[Hook]] = {
            hook_type: [] for hook_type in HookType
        }
                
    async def validate_stage_internal_hooks(self):            
        

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Checking internal Hooks for stage - {self.stage.name} - of type - {self.stage.stage_type.name}')
        
        for reserved_hook_name in self.stage.internal_hooks:

            try:
                
                base_stage_name = self.stage.__class__.__base__.__name__
                hook = registrar.reserved[base_stage_name].get(reserved_hook_name)

                assert hasattr(self.stage, reserved_hook_name)
                assert hasattr(self.stage.__class__.__base__, reserved_hook_name)
                assert isinstance(hook, Hook)
                assert hook.hook_type == HookType.INTERNAL
                
                internal_hook = getattr(self.stage, hook.shortname)
                assert inspect.getsource(internal_hook) == inspect.getsource(hook.call)

            except AssertionError:
                raise ReservedMethodError(self.stage, reserved_hook_name)
            
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Found internal Hook - {hook.name}:{hook.hook_id}- for stage - {base_stage_name}')

    async def validate_stage_hooks(self):
        methods = inspect.getmembers(self.stage, predicate=inspect.ismethod)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Checking Hooks for stage - {self.stage.name} - of type - {self.stage.stage_type}')

        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registrar.all.get(method_name)
            
            if hook:
                
                base_stage_name = self.stage.__class__.__base__.__name__
                valid_hook_types_string = ', '.join([hook_type.name for hook_type in self.stage.accepted_hook_types])

                try:
                
                    assert isinstance(hook.name, str), "Hook name must be string."
                    assert isinstance(hook.hook_id, str), "Hook ID must be string."
                    assert isinstance(hook.shortname, str), f"Hook shortname for hook {hook.name}:{hook.hook_id} must be a valid string."
                    assert isinstance(hook._call, MethodType), f"Hook {hook.name}:{hook.hook_id} call is not a vaid method. All hooks must be a method."
                    assert inspect.iscoroutinefunction(hook.call), f"Hook {hook.name}:{hook.hook_id} call is not a vaid coroutine. All hook methods must be a async."
                    assert hook.hook_type in self.stage.accepted_hook_types, f"Hook {hook.name}:{hook.hook_id} is not a valid hook type for stage {self.stage.name}. Valid hook types are {valid_hook_types_string}."
                
                except AssertionError as hook_validation_error:
                    raise HookValidationError(hook.stage_instance, str(hook_validation_error))

                self.hooks[hook.hook_type].append(hook)

                self.hooks_by_name[hook.name] = hook

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Found Hook - {hook.name}:{hook.hook_id}- of type - {hook.hook_type.name.capitalize()} - for stage - {base_stage_name}')
