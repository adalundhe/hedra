import asyncio
import inspect
from typing import Dict, List, Union
from collections import defaultdict
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.registry.registry_types import ValidateHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.validate.exceptions import ReservedMethodError

from .validators import Validator


class Validate(Stage):
    stage_type=StageTypes.VALIDATE
    
    def __init__(self) -> None:
        super().__init__()
        self.stages: Dict[StageTypes, Dict[str, Stage]] = {}
        self.validation_stages = {}
        self.hooks_by_name: Dict[str, Hook] = defaultdict(dict)

        base_stage_name = self.__class__.__name__
        self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Checking internal Hooks for stage - {base_stage_name}')

        for reserved_hook_name in self.internal_hooks:
            try:

                hook = registrar.reserved[base_stage_name].get(reserved_hook_name)

                assert hasattr(self, reserved_hook_name)
                assert isinstance(hook, Hook)
                assert hook.hook_type == HookType.INTERNAL

                internal_hook = getattr(self, hook.shortname)
                assert inspect.getsource(internal_hook) == inspect.getsource(hook.call)

            except AssertionError:
                raise ReservedMethodError(self, reserved_hook_name)

            self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Found internal Hook - {hook.name}:{hook.hook_id}- for stage - {base_stage_name}')
        
        for hook_type in HookType:
            self.hooks[hook_type] = []

        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.INTERNAL, 
            HookType.TRANSFORM,
            HookType.VALIDATE, 
        ]

    @Internal()
    async def run(self):
        
        await self.run_pre_events()

        validator = Validator(self.stages, self.metadata_string)
        await validator.validate_stages()

        validator_hooks = await self.collect_validate_hooks()
        validator.add_hooks(HookType.VALIDATE, validator_hooks)

        await validator.validate_hooks()

        await self.run_post_events()

    async def collect_validate_hooks(self):       
        
        validate_hooks: List[ValidateHook] = []

        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        for _, method in methods:
            method_name = method.__qualname__
            hook: Hook = registrar.all.get(method_name)
                        
            if hook and hook.hook_type is HookType.VALIDATE:
                validate_hooks.append(hook)
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Validation hook - {hook.name}:{hook.hook_id}:{hook.hook_id}')

        return validate_hooks