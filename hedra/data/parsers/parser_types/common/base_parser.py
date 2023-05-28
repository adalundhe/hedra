import asyncio
import re
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.timeouts import Timeouts
from typing import List, Dict, Any, Coroutine


class BaseParser:

    def __init__(
        self, 
        name: str,
        config: Config
    ) -> None:
        
        self._loop: asyncio.AbstractEventLoop = None
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')
        self.name = name
        self.config = config
        self.timeouts = Timeouts(
            connect_timeout=config.connect_timeout,
            total_timeout=config.request_timeout
        )

    async def parse(self, action_data: Dict[str, Any]) -> Coroutine[Any, Any, ActionHook]:
        raise NotImplementedError('Parse method is not implemented for base Parser class.')
    
