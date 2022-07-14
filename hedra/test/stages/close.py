from typing import Dict, List
from hedra.test.client import Client
from hedra.test.hooks.hook import Hook
from hedra.test.hooks.types import HookType
from .stage import Stage


class Close(Stage):
    client: Client = None

    def __init__(self) -> None:
        super().__init__()

    async def close(self):
        try:
            await self.client.session.close()
        
        except Exception:
            pass