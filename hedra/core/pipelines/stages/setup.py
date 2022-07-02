import asyncio
from .base_stage import BaseStage


class Setup(BaseStage):

    def __init__(self, config, persona) -> None:
        super(Setup, self).__init__(config, persona)

        self.order = 1
        self.name = b'setup'
        self.persona_type = config.executor_config.get('persona_type')
        self.engine_type = config.executor_config.get('engine_type')

    @classmethod
    def about(cls):
        return '''
        Setup Stage

        The setup stage is the first stage in a test, during which the persona creates/partitions batches, 
        engine creates any sessions required, etc.

        '''

    async def execute(self, persona):
        
        if self._no_run_visuals is False and self._is_parallel is False:
            self.session_logger.debug(f'Executing using persona type: {self.persona_type}')
            self.session_logger.debug(f'Executing using engine type - {self.engine_type}')

        self.selected_persona = persona
        await self.selected_persona.setup(self.actions)

        await asyncio.sleep(1)

        return self.selected_persona
