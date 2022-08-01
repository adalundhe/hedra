import asyncio
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.hooks.client.config import Config
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.playwright.client import MercuryPlaywrightClient
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.playwright import (
    Command,
    Page,
    URL,
    Input,
    Options
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.hooks.client.store import ActionsStore
from .base_client import BaseClient


class PlaywrightClient(BaseClient):

    def __init__(self, config: Config) -> None:
        super().__init__()
        
        self.session = MercuryPlaywrightClient(
            concurrency=config.batch_size,
            group_size=config.options.get('session_group_size', 25),
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            )
        )

        self.request_type = RequestTypes.PLAYWRIGHT
        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def run(
        self,
        command: str,
        selector: str=None,
        attribute: str=None,
        x_coordinate: int=0,
        y_coordinate: int=0,
        frame: int=0,
        location: str=None,
        headers: Dict[str, str]={},
        key: str=None,
        text: str=None,
        function: str=None,
        args: List[Any]=None,
        filepath: str=None,
        file: bytes=None,
        event: str=None,
        option: str=None,
        is_checked: bool=False,
        timeout: int=60000,
        extra: Dict[str, Any]={},
        switch_by: str='url',
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[FunctionType] = []
    ):
        if self.session.registered.get(self.next_name) is None:
            command = Command(
                self.next_name,
                command,
                page=Page(
                    selector=selector,
                    attribute=attribute,
                    x_coordinate=x_coordinate,
                    y_coordinate=y_coordinate,
                    frame=frame
                ),
                url=URL(
                    location=location,
                    headers=headers
                ),
                input=Input(
                    key=key,
                    text=text,
                    function=function,
                    args=args,
                    filepath=filepath,
                    file=file
                ),
                options=Options(
                    event=event,
                    option=option,
                    is_checked=is_checked,
                    timeout=timeout,
                    extra=extra,
                    switch_by=switch_by
                ),
                user=user,
                tags=tags,
                checks=checks
            )

            result = await self.session.prepare(command)
            if isinstance(result, Exception):
                raise result

            if self.intercept:
                self.actions.store(self.next_name, command, self.session)
                
                loop = asyncio.get_event_loop()
                self.waiter = loop.create_future()
                await self.waiter

        return self.session