import uuid
from typing import Dict, List, Union
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class SequencedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(SequencedPersona, self).__init__(config)

        self.persona_id = str(uuid.uuid4())
        self.type = PersonaTypes.SEQUENCE

    def setup(self, hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]], metadata_string: str):

        self.metadata_string = f'{metadata_string} Persona: {self.type.capitalize()}:{self.persona_id} - '
        
        sequence = sorted(
            hooks.get(HookType.ACTION),
            key=lambda action: action.metadata.order
        )

        self.actions_count = len(sequence)
        self._hooks = sequence
