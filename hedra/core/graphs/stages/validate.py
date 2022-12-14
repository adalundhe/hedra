import traceback
import inspect
import os
from pathlib import Path
from types import MethodType
from typing import Dict
from collections import defaultdict
from inspect import iscoroutinefunction
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.stages.types.stage_types import StageTypes
from .stage import Stage
from .exceptions import (
    HookValidationError,
    ReservedMethodError,
    MissingReservedMethodError
)


class Validate(Stage):
    stage_type=StageTypes.VALIDATE
    
    def __init__(self) -> None:
        super().__init__()
        self.stages: Dict[StageTypes, Dict[str, Stage]] = {}

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

            self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Found internal Hook - {hook.name} - for stage - {base_stage_name}')
        
        for hook_type in HookType:
            self.hooks[hook_type] = []

        self.accepted_hook_types = [ HookType.VALIDATE, HookType.INTERNAL ]

    @Internal()
    async def run(self):

        stages = {
            stage_type: stage for stage_type, stage in self.stages.items() if stage_type not in [StageTypes.VALIDATE, StageTypes.IDLE, StageTypes.COMPLETE]
        }

        validation_stage_names = ', '.join([stage.name for stage in stages])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating stages - {validation_stage_names}')
        await self.logger.spinner.append_message(f'Validating - {len(stages)} - stages')
        
        hooks_by_name: Dict[str, Hook] = defaultdict(dict)

        for stages_types in stages.values():
            for stage in stages_types.values():
                stage.context = self.context

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating stage - {stage.name} - of stage type - {stage.stage_type.name}')
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Checking internal Hooks for stage - {stage.name} - of type - {stage.stage_type.name}')

                for reserved_hook_name in stage.internal_hooks:

                    try:
                        
                        base_stage_name = stage.__class__.__base__.__name__
                        hook = registrar.reserved[base_stage_name].get(reserved_hook_name)

                        assert hasattr(stage, reserved_hook_name)
                        assert hasattr(stage.__class__.__base__, reserved_hook_name)
                        assert isinstance(hook, Hook)
                        assert hook.hook_type == HookType.INTERNAL
                        
                        internal_hook = getattr(stage, hook.shortname)
                        assert inspect.getsource(internal_hook) == inspect.getsource(hook.call)

                    except AssertionError:
                        raise ReservedMethodError(stage, reserved_hook_name)

                    
                    await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Found internal Hook - {hook.name} - for stage - {base_stage_name}')

                methods = inspect.getmembers(stage, predicate=inspect.ismethod)

                for hook_type in HookType:
                    stage.hooks[hook_type] = [] 

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Checking Hooks for stage - {stage.name} - of type - {stage.stage_type}')

                for _, method in methods:

                    method_name = method.__qualname__
                    hook: Hook = registrar.all.get(method_name)
                    
                    if hook:

                        valid_hook_types_string = ', '.join([hook_type.name for hook_type in stage.accepted_hook_types])

                        try:
                        
                            assert isinstance(hook.name, str), "Hook name must be string."
                            assert isinstance(hook.shortname, str), f"Hook shortname for hook {hook.name} must be a valid string."
                            assert isinstance(hook.call, MethodType), f"Hook {hook.name} call is not a vaid method. All hooks must be a method."
                            assert iscoroutinefunction(hook.call), f"Hook {hook.name} call is not a vaid coroutine. All hook methods must be a async."
                            assert hook.hook_type in stage.accepted_hook_types, f"Hook {hook.name} is not a valid hook type for stage {stage.name}. Valid hook types are {valid_hook_types_string}."
                        
                        except AssertionError as hook_validation_error:
                            raise HookValidationError(stage, str(hook_validation_error))

                        hook.stage = stage.name
                        stage.hooks[hook.hook_type].append(hook)
                        self.hooks[hook.hook_type].append(hook)

                        hooks_by_name[hook.name] = hook

                        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Found Hook - {hook.name} - of type - {hook.hook_type.name.capitalize()} - for stage - {base_stage_name}')

        methods = inspect.getmembers(self, predicate=inspect.ismethod)
        for _, method in methods:
            method_name = method.__qualname__
            hook: Hook = registrar.all.get(method_name)
                        
            if hook and hook.hook_type is HookType.VALIDATE:
                self.hooks[HookType.VALIDATE].append(hook)

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Validation hook - {hook.name}')

        try:
            
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.SETUP.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.SETUP):
                assert hook.hook_type is HookType.SETUP, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.SETUP.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @setup hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to @setup hook - {hook.name}"
                assert hook.call.__code__.co_argcount == 1, f"Too many args. - @setup hook {hook.name} requires no additional args."
                assert 'self' in hook.call.__code__.co_varnames

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.BEFORE.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.BEFORE):
                assert hook.hook_type is HookType.BEFORE, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.BEFORE.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @before hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to hook - {hook.name}"
                assert hook.call.__code__.co_argcount > 2, f"Missing required argument 'action' for @before hook {hook.name}"
                assert hook.call.__code__.co_argcount < 4, f"Too many args. - @before hook {hook.name} only requires 'action' as additional arg."
                assert len(hook.names) > 0, f"No target hook names provided for @before hook {hook.name}. Please specify at least one hook to validate."
                assert 'self' in hook.call.__code__.co_varnames

                for name in hook.names:
                    hook_for_validation = hooks_by_name.get(
                        f'{hook.stage}.{name}'
                    )

                    assert hook_for_validation is not None, f"Specified hook {name} for stage {hook.stage} not found for @before hook {hook.name}"

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.ACTION.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.ACTION):
                assert hook.hook_type is HookType.ACTION, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.ACTION.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @action hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to @action hook - {hook.name}"
                assert hook.call.__code__.co_argcount == 1, f"Too many args. - @action hook {hook.name} requires no additional args."
                assert 'self' in hook.call.__code__.co_varnames

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.AFTER.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.AFTER):
                assert hook.hook_type is HookType.AFTER, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.AFTER.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @after hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to @after hook - {hook.name}"
                assert hook.call.__code__.co_argcount > 2, f"Missing required argument 'result' for @after hook {hook.name}"
                assert hook.call.__code__.co_argcount < 4, f"Too many args. - @after hook {hook.name} only requires 'action' and 'result' as additional args."
                assert len(hook.names) > 0, f"No target hook names provided for @after hook {hook.name}. Please specify at least one hook to validate."
                assert 'self' in hook.call.__code__.co_varnames

                for name in hook.names:
                    hook_for_validation = hooks_by_name.get(
                        f'{hook.stage}.{name}'
                    )

                    assert hook_for_validation is not None, f"Specified hook {name} for stage {hook.stage} not found for @after hook {hook.name}"

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.TEARDOWN.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.TEARDOWN):
                assert hook.hook_type is HookType.TEARDOWN, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.TEARDOWN.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @teardown hook {hook.name}."
                assert hook.shortname in hook.name, "Shortname must be contained in full Hook name."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to @teardown hook - {hook.name}"
                assert hook.call.__code__.co_argcount == 1, f"Too many args. - @teardown hook {hook.name} requires no additional args."
                assert 'self' in hook.call.__code__.co_varnames

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.SAVE.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.SAVE):
                

                assert hook.hook_type is HookType.SAVE, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.SAVE.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @save hook {hook.name}."
                
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to @save hook - {hook.name}"
                assert 'self' in hook.call.__code__.co_varnames

                if hook.config.path:
                    hook_path_dir = str(Path(hook.config.path).parent.resolve())
                    
                    assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'data' for @save hook {hook.name}"
                    assert hook.call.__code__.co_argcount < 3, f"Too many args. - @save hook {hook.name} only requires 'data' as additional args."
                    assert isinstance(hook.config.path, str), f"Invalid path type - @save hook {hook.name} path must be a valid string."
                    assert os.path.exists(hook_path_dir), f"Invalid path - @save hook {hook.name} path {hook_path_dir} must exist."

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.CHECK.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.CHECK):
                assert hook.hook_type is HookType.CHECK, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.CHECK.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @check hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to hook - {hook.name}"
                assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'event' for @check hook {hook.name}"
                assert hook.call.__code__.co_argcount < 3, f"Too many args. - @check hook {hook.name} only requires 'event' as additional arg."
                assert 'self' in hook.call.__code__.co_varnames

                for name in hook.names:
                    hook_for_validation = hooks_by_name.get(
                        f'{hook.stage}.{name}'
                    )

                    assert hook_for_validation is not None, f"Specified hook {name} for stage {hook.stage} not found for @check hook {hook.name}"

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.METRIC.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.METRIC):
                assert hook.hook_type is HookType.METRIC, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.METRIC.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @metric hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to hook - {hook.name}"
                assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'events_group' for @metric hook {hook.name}"
                assert hook.call.__code__.co_argcount < 3, f"Too many args. - @metric hook {hook.name} only requires 'events_group' as additional arg."
                assert 'self' in hook.call.__code__.co_varnames

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {HookType.VALIDATE.name.capitalize()} - hooks')

            for hook in self.hooks.get(HookType.VALIDATE):
                assert hook.hook_type is HookType.VALIDATE, f"Hook type mismatch - hook {hook.name} is a {hook.hook_type.name} hook, but Hedra expected a {HookType.VALIDATE.name} hook."
                assert hook.shortname in hook.name, f"Shortname {hook.shortname} must be contained in full Hook name {hook.name} for @validate hook {hook.name}."
                assert hook.call is not None, f"Method is not not found on stage or was not supplied to hook - {hook.name}"
                assert hook.call.__code__.co_argcount > 1, f"Missing required argument 'hook' for @validate hook {hook.name}"
                assert hook.call.__code__.co_argcount < 3, f"Too many args. - @validate hook {hook.name} only requires 'events_group' as additional arg."
                assert hook.stage is not None, f"No target stage name provided for @validate hook {hook.name}. Please specify the stage name whose hooks you wish to validate."
                assert len(hook.names) > 0, f"No target hook names provided for @validate hook {hook.name}. Please specify at least one hook to validate."
                assert 'self' in hook.call.__code__.co_varnames

                for name in hook.names:
                    hook_for_validation = hooks_by_name.get(name)
                    stage, hook_name = name.split('.')

                    assert hook_for_validation is not None, f"Specified hook {hook_name} for stage {stage} not found for @validate hook {hook.name}"

                    await hook.call(hook_for_validation.call)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validation for stages - {validation_stage_names} - complete')
            await self.logger.spinner.set_default_message(f'Validated - {len(stages)} stages')

        except AssertionError as hook_validation_error:
            raise HookValidationError(stage, str(hook_validation_error))