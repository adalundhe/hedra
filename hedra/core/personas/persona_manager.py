from distutils.command.config import config
from .types.default_persona import DefaultPersona
from .types.batched_persona import BatchedPersona
from .types.sequenced_persona import SequencedPersonaCollection
from .types.multi_sequence_persona import MultiSequencePersona
from .types.ramped_persona import RampedPersona
from .types.ramped_interval_persona import RampedIntervalPersona
from .types.weighted_selection_persona import WeightedSelectionPersona
from .types.cyclic_nowait_persona import CyclicNoWaitPersona
from hedra.core.engines.client.config import Config


registered_personas = {
    'default': lambda config: DefaultPersona(config),
    'batched': lambda config: BatchedPersona(config),
    'multi-sequence': lambda config: MultiSequencePersona(config),
    'ramped': lambda config: RampedPersona(config),
    'ramped-interval': lambda config: RampedIntervalPersona(config),
    'sequence': lambda config: SequencedPersonaCollection(config),
    'weighted': lambda config: WeightedSelectionPersona(config),
    'no-wait': lambda config: CyclicNoWaitPersona(config)
}

def get_persona(config: Config):
    return registered_personas.get(
        config.persona_type, 
        DefaultPersona
    )(config)
