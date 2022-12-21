
from collections import defaultdict
from typing import Dict, List
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    TaskHook,
    ValidateHook
)
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.logging import HedraLogger
from .hook_validators import (
    ActionHookValidator,
    AfterHookValidator,
    BaseHookVaidator,
    BeforeHookVaidator,
    ChannelHookValidator,
    CheckHookValidator,
    EventHookVaidator,
    MetricHookVaidator,
    RestoreHookValidator,
    SaveHookValidator,
    SetupHookVaidator,
    TaskHookValidator,
    TeardownHookValidator,
    ValidateHookValidator
)
from .stage_validator import StageValidator



class Validator:

    def __init__(self, stages: Dict[str, Dict[str, Stage]], metadata_string: str) -> None:


        self.logger = HedraLogger()
        self.logger.initialize()
        self.metadata_string: str = metadata_string

        self.hooks_by_name: Dict[str, Hook] = defaultdict(dict)
        self.custom_validation_hooks: List[ValidateHook] = []

        self._validators = {
            HookType.ACTION: ActionHookValidator(metadata_string),
            HookType.AFTER: AfterHookValidator(metadata_string),
            HookType.BEFORE: BeforeHookVaidator(metadata_string),
            HookType.CHANNEL: ChannelHookValidator(metadata_string),
            HookType.CHANNEL: CheckHookValidator(metadata_string),
            HookType.EVENT: EventHookVaidator(metadata_string),
            HookType.METRIC: MetricHookVaidator(metadata_string),
            HookType.RESTORE: RestoreHookValidator(metadata_string),
            HookType.SAVE: SaveHookValidator(metadata_string),
            HookType.SETUP: SetupHookVaidator(metadata_string),
            HookType.TASK: TaskHookValidator(metadata_string),
            HookType.TEARDOWN: TeardownHookValidator(metadata_string),
            HookType.VALIDATE: ValidateHookValidator(metadata_string)
        }

        self.validation_stages = {
            stage_type: stage for stage_type, stage in stages.items() if stage_type not in [
                StageTypes.VALIDATE, 
                StageTypes.IDLE, 
                StageTypes.COMPLETE
            ]
        }

        self.stage_names: List[str] = []
        for stage_type in self.validation_stages:
            self.stage_names.extend(self.validation_stages[stage_type])

        self.hooks = {
            hook_type: [] for hook_type in HookType
        }


        self.stage_validators: List[StageValidator] = []

        for stage_type in self.validation_stages:
            for stage_name in self.validation_stages[stage_type]:
                stage = self.validation_stages[stage_type].get(stage_name)
                self.stage_validators.append(StageValidator(stage, metadata_string))

    def add_hooks(self, hook_type: HookType, hooks: List[Hook]):
        self.hooks[hook_type].extend(hooks)

    async def validate_stages(self):

        validation_stage_names = ', '.join(self.stage_names)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating stages - {validation_stage_names}')
        await self.logger.spinner.append_message(f'Validating - {len(self.validation_stages)} - stages')
        
        for stage_validator in self.stage_validators:
            await stage_validator.validate_stage_internal_hooks()
            await stage_validator.validate_stage_hooks()

            for hook_type, hooks in stage_validator.hooks.items():
                self.hooks[hook_type].extend(hooks)

            self.hooks_by_name.update(stage_validator.hooks_by_name)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validation for stages - {validation_stage_names} - complete')
        await self.logger.spinner.set_default_message(f'Validated - {len(self.stage_names)} stages')

    async def validate_hooks(self):

        channel_hook_validator: ChannelHookValidator = self._validators[HookType.CHANNEL]
        channel_hook_validator.action_and_task_hooks = [
            *self.hooks.get(HookType.ACTION),
            *self.hooks.get(HookType.TASK)
        ]

        self._validators[HookType.CHANNEL] = channel_hook_validator

        for hook_type in self.hooks:
            
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Validating - {hook_type.name.capitalize()} - hooks')

            validator: BaseHookVaidator = self._validators.get(hook_type)
            if validator:
                validator.hooks_by_name = self.hooks_by_name
                
                for hook in self.hooks[hook_type]:
                    await validator.validate(hook)


        