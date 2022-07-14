from .types.default_persona import DefaultPersona
from .types.batched_persona import BatchedPersona
from .types.sequenced_persona import SequencedPersonaCollection
from .types.multi_sequence_persona import MultiSequencePersona
from .types.ramped_persona import RampedPersona
from .types.ramped_interval_persona import RampedIntervalPersona
from .types.weighted_selection_persona import WeightedSelectionPersona
from .types.cyclic_nowait_persona import CyclicNoWaitPersona


class PersonaManager:

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

    def __init__(self, config, handler):
        self.config = config.executor_config
        self.type = self.config.get('persona_type', 'simple')
        self.selected_persona = self.registered_personas.get(self.type, DefaultPersona)(
            config,
            handler
        )

    @classmethod
    def about(cls):

        persona_names = '\n\t'.join([f'- {persona_name}' for persona_name in cls.registered_personas])

        return f'''
        Personas:

        key arguments:

        --persona <persona_type_to_use>

        --total-time <string_total_runtime_in_HH:MM:SS> (defaults to 1 minute [00:01:00])

        Personas dictate "how" actions execute - including ordering, batching, waits between
        batches, ramping, etc. Personas are purely a logic layer, and are composed of the following 
        main methods.
        
        - setup (Initializes the Persona and attaches the engine of the specified type to the persona. Accepts a single positional argument for parsed actions)

        - execute (Iterates over the created batches [sometimes creating a new batch after iteration] and passes batches of actions to the engine for execution, returning unaggregated results)

        - close (Calls the close method of the engine, signaling the end of test execution)
        
        Hedra currently offers the following personas:

        {persona_names}

        For more information on each persona, run the command:

            hedra --about personas:<persona_name>


        For more information on batches run the command:

            hedra --about personas:batches


        Related Topics:

        - batches
        - engines
        - optimizers

        '''

    @classmethod
    def about_batches(cls, batches_about_item=None):

        if batches_about_item == "intervals":
            return DefaultPersona.about_batch_intervals()

        return DefaultPersona.about_batches()


