import asyncio
from .base_stage import BaseStage


class Results(BaseStage):

    def __init__(self, config, persona) -> None:
        super(Results, self).__init__(config, persona)
        self.order = 5
        self.name = b'results'
        self.results = []
        self.stats = {}

    @classmethod
    def about(cls):
        return '''
        Results Stage


        The results stage is the final stage in test execution. It occurs post-execution and is responsible for
        retrieving stored action results and statistic from the Persona used in the execution stage.
        
        '''

    async def execute(self, persona):
        self.selected_persona = persona
        self.results = self.selected_persona.results
        self.stats = self.selected_persona.stats

        await asyncio.sleep(1)

        return self.selected_persona