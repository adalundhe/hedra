import time
from pathlib import Path
from typing import Dict, Literal, Optional, Sequence

from playwright.async_api import FileChooser, FilePayload

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.file_chooser import SetFilesCommand
from .models.results import PlaywrightResult


class BrowserFileChooser:
    def __init__(
        self,
        file_chooser: FileChooser,
        timeouts: Timeouts,
        metadata: BrowserMetadata,
        url: str,
    ) -> None:
        self.file_chooser = file_chooser
        self.timeouts = timeouts
        self.metadata = metadata
        self.url = url

    async def set_files(
        self,
        files: str | Path | FilePayload | Sequence[str | Path] | Sequence[FilePayload],
        no_wait_after: Optional[bool] = None,
        timeout: Optional[int | float] = None,
    ):
        timings: Dict[Literal["command_start", "command_end"], float] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        command = SetFilesCommand(
            files=files,
            no_wait_after=no_wait_after,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings["command_start"] = time.monotonic()

        try:
            await self.file_chooser.set_files(
                command.files,
                no_wait_after=command.no_wait_after,
                timeout=command.timeout,
            )

        except Exception as err:
            timings["command_end"] = time.monotonic()

            return PlaywrightResult(
                command="set_files",
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url,
            )

        timings["command_end"] = time.monotonic()

        return PlaywrightResult(
            command="set_files",
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url,
        )
