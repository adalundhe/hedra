from re import S
from async_tools.datatypes.async_dict import AsyncDict
from .actions import Action
from async_tools.datatypes import AsyncList


class MultiUserSequenceParser:

    def __init__(self, config):
        self._raw_actions = config.actions
        self.sorted = config.executor_config.get('sorted')
        self.engine_type = config.executor_config.get('engine_type')
        self.sequences = {}

    @classmethod
    def about(cls):
        return '''
        Multi User Sequence Parser - (multi-user-sequence)

        The Multi User Sequence parser is automatically selected and used when the Multi User Sequence 
        persona is selected and the Engine type selected is *not* the Action Set engine. The Multi 
        User Sequence parser's primary function is to both parse actions and provide the multiple sorted 
        sequences for each user to the Multi User Sequence persona prior to execution.

        '''

    def __len__(self):
        return len(self.sequences)

    def __iter__(self):
        for user in self.sequences:
            yield user

    def __getitem__(self, user):
        return self.sequences.get(user)

    async def users(self):
        return AsyncList([user for user in self.sequences])

    async def user_sequences(self, user):
        return self.sequences[user]

    async def sort(self):
        sorted_sequences = {}
        
        for user in self.sequences:

            sorted_user_sequences = {}

            for sequence in self.sequences[user]:
                unsorted_sequence = self.sequences[user][sequence]
                sorted_sequence = AsyncList(unsorted_sequence)

                sorted_sequence = await sorted_sequence.sort(key=lambda action: action.order)

                sorted_user_sequences[sequence] = sorted_sequence

            sorted_sequences[user] = sorted_user_sequences

        return sorted_sequences

    async def parse(self):
        for user in self._raw_actions:
            self.sequences[user] = {}
            user_sequence = self._raw_actions.get(user)
            for group in user_sequence:
                actions = []
                self.sequences[user][group] = []
                for action in user_sequence.get(group):
                    http_action = Action(action, self.engine_type, group=group)
                    http_action = http_action.type
                    http_action.user = user
                    await http_action.parse_data()
                    actions.append(http_action)
            
                self.sequences[user][group] = actions
      
        return self.sequences
