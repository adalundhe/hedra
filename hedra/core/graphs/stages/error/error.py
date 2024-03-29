import inspect
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.graphs.stages.base.exceptions.reserved_method_error import ReservedMethodError
from hedra.core.graphs.stages.base.stage import Stage


class Error(Stage):
    stage_type=StageTypes.ERROR

    def __init__(self) -> None:
        super().__init__()
        self.error = None
        self.retries: int = 0

        self.priority = None
        self.priority_level: StagePriority = StagePriority.map(
            self.priority
        )

        base_stage_name = self.__class__.__name__
        self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Checking internal Hooks for stage - {base_stage_name}')
        
        for reserved_hook_name in self.internal_hooks:
            try:

                hook = registrar.reserved[base_stage_name].get(reserved_hook_name)

                assert hasattr(self, reserved_hook_name) is True
                assert isinstance(hook, Hook) is True
                assert hook.hook_type == HookType.INTERNAL

                internal_hook = getattr(self, hook.shortname)
                assert inspect.getsource(internal_hook) == inspect.getsource(hook._call)

            except AssertionError:
                raise ReservedMethodError(self, reserved_hook_name)

            hook._call = hook._call.__get__(self, self.__class__)
            setattr(self, reserved_hook_name, hook._call)
            
            self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Loading internal Hook - {hook.name} - for stage - {base_stage_name}')

    @Internal()
    async def run(self):
        await self.logger.spinner.system.error(f'{self.metadata_string} - Encountered error - {self.error}')
        await self.logger.filesystem.aio['hedra.core'].error(f'{self.metadata_string} - Encountered error - {self.error}')
        


