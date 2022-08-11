from distutils.command.config import config
from .types.default_persona import DefaultPersona
from .types.batched_persona import BatchedPersona
from .types.sequenced_persona import SequencedPersonaCollection
from .types.ramped_persona import RampedPersona
from .types.ramped_interval_persona import RampedIntervalPersona
from .types.weighted_selection_persona import WeightedSelectionPersona
from .types.constant_arrival_rate import ConstantArrivalPersona
from .types.cyclic_nowait_persona import CyclicNoWaitPersona
from .types import PersonaTypes
from hedra.core.engines.client.config import Config


registered_personas = {
    PersonaTypes.DEFAULT: lambda config: DefaultPersona(config),
    PersonaTypes.BATCHED: lambda config: BatchedPersona(config),
    PersonaTypes.RAMPED: lambda config: RampedPersona(config),
    PersonaTypes.RAMPED_INTERVAL: lambda config: RampedIntervalPersona(config),
    PersonaTypes.CONSTANT_ARRIVAL: lambda config: ConstantArrivalPersona(config),
    PersonaTypes.SEQUENCE: lambda config: SequencedPersonaCollection(config),
    PersonaTypes.WEIGHTED: lambda config: WeightedSelectionPersona(config),
    PersonaTypes.NO_WAIT: lambda config: CyclicNoWaitPersona(config)
}

def get_persona(config: Config):
    return registered_personas.get(
        config.persona_type, 
        DefaultPersona
    )(config)
