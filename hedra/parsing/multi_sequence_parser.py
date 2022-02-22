from zebra_async_tools.functions.awaitable import awaitable
from .actions import Action
from zebra_async_tools.datatypes import AsyncList


class MultiSequenceParser:

    def __init__(self, config):
        self._raw_actions = config.actions
        self.sorted = config.executor_config.get('sorted')
        self.engine_type = config.executor_config.get('engine_type')
        self.sequences = {}

    @classmethod
    def about(cls):
        return '''
        Multi Sequence Parser - (multi-sequence)

        The Multi Sequence parser is automatically selected and used when the Multi Sequence persona
        is selected and the Engine type selected is *not* the Action Set engine. The Multi Sequence 
        parser's primary function is to both parse actions and provide the multiple sorted sequences
        to the Multi Sequence persona prior to execution.

        '''

    def __len__(self):
        return len(self.sequences)

    def __iter__(self):
        for sequence in self.sequences:
            yield sequence

    def __getitem__(self, sequence):
        return self.sequences.get(sequence)

    async def count(self):
        return await awaitable(len, self.sequences)

    async def iter_sequences(self):
        sequences = AsyncList(self.sequences.values())
        async for sequence in sequences:
            yield sequence

    async def parse(self):
        for group in self._raw_actions:
            actions = []
            for action in self._raw_actions.get(group):
                http_action = Action(action, self.engine_type, group=group)
                http_action = http_action.type
                http_action.user = http_action.user
                await http_action.parse_data()
                actions.append(http_action)
        
            self.sequences[group] = actions
        
        return self.sequences

    async def sort(self):
        sorted_sequences = {}
        for sequence in self.sequences:
            unsorted_sequence = self.sequences[sequence]
            sorted_sequence = AsyncList(unsorted_sequence)

            sorted_sequence = await sorted_sequence.sort(key=lambda action: action.order)

            sorted_sequences[sequence] = sorted_sequence

        return sorted_sequences
