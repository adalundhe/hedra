from typing import Dict, List
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class SequencedPersonaCollection(DefaultPersona):

    def __init__(self, config: Config):
        super(SequencedPersonaCollection, self).__init__(config)
        self.type = PersonaTypes.SEQUENCE

    async def setup(self, hooks: Dict[HookType, List[Hook]]):
        
        sequence = sorted(
            hooks.get(HookType.ACTION),
            key=lambda action: action.config.order
        )

        self.actions_count = len(sequence)
        self._hooks = sequence
