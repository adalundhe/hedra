from .types.default_persona import DefaultPersona
from .types.batched_persona import BatchedPersona
from .types.sequenced_persona import SequencedPersonaCollection
from .types.multi_sequence_persona import MultiSequencePersona
from .types.ramped_persona import RampedPersona
from .types.ramped_interval_persona import RampedIntervalPersona
from .types.weighted_selection_persona import WeightedSelectionPersona
from .types.cyclic_nowait_persona import CyclicNoWaitPersona
from hedra.core.hooks.client.config import Config


registered_personas = {
    'default': DefaultPersona,
    'batched': BatchedPersona,
    'multi-sequence': MultiSequencePersona,
    'ramped': RampedPersona,
    'ramped-interval': RampedIntervalPersona,
    'sequence': SequencedPersonaCollection,
    'weighted': WeightedSelectionPersona,
    'no-wait': CyclicNoWaitPersona
}

def get_persona(config: Config):
    return registered_personas.get(
        config.persona_type, 
        DefaultPersona
    )(config)
