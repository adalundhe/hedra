from enum import Enum


class PersonaTypes:
    BATCHED='BATCHED'
    DEFAULT='DEFAULT'
    RAMPED='RAMPED'
    RAMPED_INTERVAL='RAMPED_INTERVAL'
    CONSTANT_ARRIVAL='CONSTANT_ARRIVAL'
    CONSTANT_SPAWN='CONSTANT_SPAWN'
    SEQUENCE='SEQUENCE'
    WEIGHTED='WEIGHTED'
    NO_WAIT='NO-WAIT'


class PersonaTypesMap:

    def __init__(self) -> None:
        self.types = {
            'batched': PersonaTypes.BATCHED,
            'default': PersonaTypes.DEFAULT,
            'ramped': PersonaTypes.RAMPED,
            'constant-arrival': PersonaTypes.CONSTANT_ARRIVAL,
            'constant-spawn': PersonaTypes.CONSTANT_SPAWN,
            'sequence': PersonaTypes.SEQUENCE,
            'weighted': PersonaTypes.WEIGHTED,
            'no-wait': PersonaTypes.NO_WAIT
        }

    def __getitem__(self, persona_type: str):
        return self.types.get(persona_type, PersonaTypes.DEFAULT)