import uuid
from typing import Dict, List
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class SequencedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(SequencedPersona, self).__init__(config)

        self.persona_id = str(uuid.uuid4())
        self.type = PersonaTypes.SEQUENCE

    def setup(self, hooks: Dict[HookType, List[Hook]], metadata_string: str):

        self.metadata_string = f'{metadata_string} Persona: {self.type.capitalize()}:{self.persona_id} - '
        
        sequence = sorted(
            hooks.get(HookType.ACTION),
            key=lambda action: action.config.order
        )

        self.actions_count = len(sequence)
        self._hooks = sequence