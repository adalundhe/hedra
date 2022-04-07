from zebra_async_tools.datatypes import AsyncList


class SequenceStep:

    def __init__(self, wait_interval=None) -> None:
        self.actions = AsyncList()
        self.wait_interval = wait_interval

    async def add_action(self, action):
        await self.actions.append(action)