import uuid
from typing import Dict, List, Union
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class SequencedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(SequencedPersona, self).__init__(config)

        self.persona_id = str(uuid.uuid4())
        self.type = PersonaTypes.SEQUENCE

    def setup(self, hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]], metadata_string: str):

        
        self._setup(hooks, metadata_string)
        
        sequence = sorted(self._hooks,
            key=lambda action: action.metadata.order
        )

        self._hooks = sequence
        self.actions_count = len(sequence)
