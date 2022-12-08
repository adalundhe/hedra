from typing import Any, Dict, List
from hedra.plugins.types.engine import (
    EnginePlugin,
    Action,
    Result,
    connect,
    execute,
    close
)


class CustomAction(Action):

    def __init__(
        self, 
        name: str, 
        source: str = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        use_security_context: bool = False
    ) -> None:
        super(
            CustomAction,
            self
        ).__init__(
            name=name, 
            source=source, 
            user=user, 
            tags=tags, 
            use_security_context=use_security_context
        )


class CustomResult(Result):

    def __init__(
        self, 
        action: CustomAction, 
        error: Exception = None
    ) -> None:
        super().__init__(
            action, 
            error
        )


class CustomEngine(EnginePlugin[CustomAction, CustomResult]):
    cache: Dict[str, Any] = {}
    action = CustomAction
    result = CustomResult

    @connect()
    async def connect_engine(self, action: CustomAction):
        pass

    @execute()
    async def execute_action(self, action: CustomAction, result: CustomResult):
        pass

    @close()
    async def close_engine(self):
        pass