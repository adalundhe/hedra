from zebra_async_tools.datatypes.async_list import AsyncList
from zebra_automate_logging import Logger
from .responses import Response


class ParallelHandler:

    def __init__(self, config):
        self.config = config
        self.runner_mode = self.config.runner_mode

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')

    async def serialize(self, actions):
        parsed_actions = []
        async for action in AsyncList(actions):
            parsed_batch = await self._serialize_action(action)
            parsed_actions.append(parsed_batch)
        return parsed_actions

    async def _serialize_action(self, action):
            response = Response(action)
            await response.assert_response()
            return await response.to_dict()
            