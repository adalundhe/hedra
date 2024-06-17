from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Literal, Optional

from playwright.async_api import JSHandle

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.js_handle import EvaluateCommand
from .models.results import PlaywrightResult


class BrowserJSHandle:
    def __init__(
        self,
        js_handle: JSHandle,
        timeouts: Timeouts,
        metadata: BrowserMetadata,
        url: str,
    ) -> None:
        self.js_handle = js_handle
        self.timeouts = timeouts
        self.metadata = metadata
        self.url = url

    async def get_properties(self):
        properties: Dict[str, BrowserJSHandle] = {}
        handle_properties = await self.js_handle.get_properties()

        for property, handle in handle_properties.items():
            properties[property] = BrowserJSHandle(
                handle,
                self.timeouts,
                self.metadata,
                self.url,
            )

        return properties

    async def get_property(self, name: str):
        handle_property = await self.js_handle.get_property(name)
        return BrowserJSHandle(handle_property, self.timeouts, self.metadata, self.url)

    async def json_value(self):
        return await self.js_handle.json_value()

    async def dispose(self):
        await self.js_handle.dispose()

    async def evaluate(
        self,
        expression: str,
        arg: Optional[Any] = None,
        timeout: Optional[int | float] = None,
    ):
        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        timings: Dict[Literal["command_start", "command_end"], float] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        command = EvaluateCommand(expression, arg=arg, timeout=timeout)

        result: Any = None
        err: Optional[Exception] = None

        timings["command_start"] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.js_handle.evaluate(command.expression, arg=command.arg),
                timeout=command.timeout,
            )

        except Exception as err:
            timings["command_end"] = time.monotonic()

            return PlaywrightResult(
                command="evaluate",
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url,
            )

        timings["command_end"] = time.monotonic()

        return PlaywrightResult(
            command="evaluate",
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url,
        )

    async def evaluate_handle(
        self,
        expression: str,
        arg: Optional[Any] = None,
        timeout: Optional[int | float] = None,
    ):
        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        timings: Dict[Literal["command_start", "command_end"], float] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        command = EvaluateCommand(expression, arg=arg, timeout=timeout)

        result: BrowserJSHandle = None
        err: Optional[Exception] = None

        timings["command_start"] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.js_handle.evaluate_handle(command.expression, arg=command.arg),
                timeout=command.timeout,
            )

        except Exception as err:
            timings["command_end"] = time.monotonic()

            return PlaywrightResult(
                command="evaluate_handle",
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url,
            )

        timings["command_end"] = time.monotonic()

        return PlaywrightResult(
            command="evaluate_handle",
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url,
        )
