from zebra_automate_logging import Logger
from .multi_user_sequence_persona import MultiUserSequenceParser
from .multi_sequence_parser import MultiSequenceParser
from .default_parser import DefaultParser
from .action_set_parser import ActionSetParser
from zebra_async_tools.datatypes import AsyncList

class ActionsParser:

    parsers = {
        'multi-sequence': MultiSequenceParser,
        'multi-user-sequence': MultiUserSequenceParser,
        'sequence': DefaultParser,
        'default': DefaultParser,
        'action-set': ActionSetParser
    }

    def __init__(self, config):
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        engine_type = config.executor_config.get('engine_type')

        if engine_type == 'action-set':
            self.parser = self.parsers.get('action-set')(config)
        else:
            persona_type = config.executor_config.get('persona_type')
            self.parser = self.parsers.get(
                persona_type, 
                DefaultParser
            )(config)

        self.actions = {}
        self.engine = self.parser.engine_type

    @classmethod
    def about(cls):

        registered_parsers = '\n\t'.join([ f'- {parser_type}' for parser_type in cls.parsers ])

        return f'''
        Action Parsers

        key-arguments:

        --engine (The engine type to use - determines the action type the parser will use)
        --persona (The parser type to use - determines the parser used)

        Actions, whether written Python or read-in as JSON file data, need to be parsed prior to
        execution so Personas can easily organize and Engines easily execute them. Action parsers 
        are responsible for handling this task, allowing Hedra to implement a consistent interface
        both for declaring/describing actions and for processing/consuming actions internally.

        All parsers share the following methods:

        - parse (maps raw action data to the required format for Hedra to consume during execution)

        Currently registered parsers include:

        {registered_parsers}
        
        For more information on parsers, run the command:

            hedra --about actions:parsers:<parser_type>


        Related Topics:

        -personas
        -engines
        -actions

        '''

    def __len__(self):
        return self.parser.__len__()

    def __iter__(self):
        return self.parser.__iter__()

    def __getitem__(self, index):
        return self.parser.__getitem__(index)

    def __setitem__(self, key, value):
        self.actions[key] = value

    def get(self, key):
        return self.actions.get(key)

    async def to_async_list(self):
        return AsyncList(self.parser.actions)

    async def parse(self):
        return await self.parser.parse()
