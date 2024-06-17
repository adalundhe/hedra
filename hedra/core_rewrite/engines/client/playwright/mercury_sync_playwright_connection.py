import asyncio
from collections import deque
from typing import (
    Any,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
)

from playwright.async_api import (
    BrowserContext,
    Geolocation,
    async_playwright,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .browser_page import BrowserPage
from .browser_session import BrowserSession
from .models.commands.page import CloseCommand
from .models.results import PlaywrightResult


class MercurySyncPlaywrightConnection:
    def __init__(self, pool_size: int = 10**3, pages: int = 1) -> None:
        self.pool_size = pool_size
        self.pages = pages
        self.config = {}
        self.context: Optional[BrowserContext] = None
        self.sessions: Deque[BrowserSession] = deque()
        self.sem = asyncio.Semaphore(self.pool_size)
        self.timeouts = Timeouts()
        self.results: List[PlaywrightResult] = []
        self._active: Deque[Tuple[BrowserSession, BrowserPage]] = deque()

    async def open_page(self):
        await self.sem.acquire()

        session = self.sessions.popleft()
        page = await session.next_page()

        self._active.append((session, page))

        return page

    def close_page(self):
        session, page = self._active.popleft()

        session.return_page(page)
        self.sessions.append(session)

        self.sem.release()

    async def __aenter__(self):
        await self.sem.acquire()

        session = self.sessions.popleft()
        page = await session.next_page()

        self._active.append((session, page))

        return page

    async def __aexit__(self, exc_t: Type[Exception], exc_v: Exception, exc_tb: str):
        session, page = self._active.popleft()

        session.return_page(page)
        self.sessions.append(session)

        self.sem.release()

    async def start(
        self,
        browser_type: Literal["safari", "webkit", "firefox", "chrome"] = None,
        device_type: str = None,
        locale: str = None,
        geolocation: Geolocation = None,
        permissions: List[str] = None,
        color_scheme: str = None,
        options: Dict[str, Any] = {},
    ):
        playwright = await async_playwright().start()

        self.sessions.extend(
            [
                BrowserSession(playwright, self.pages, self.timeouts)
                for _ in range(self.pool_size)
            ]
        )

        await asyncio.gather(
            *[
                session.open(
                    browser_type=browser_type,
                    device_type=device_type,
                    locale=locale,
                    geolocation=geolocation,
                    permissions=permissions,
                    color_scheme=color_scheme,
                    options=options,
                    timeout=self.timeouts.request_timeout,
                )
                for session in self.sessions
            ]
        )

    async def close(
        self,
        run_before_unload: Optional[bool] = None,
        reason: Optional[str] = None,
        timeout: Optional[int | float] = None,
    ):
        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        command = CloseCommand(
            run_before_unload=run_before_unload, reason=reason, timeout=timeout
        )

        await asyncio.gather(
            *[
                session.close(
                    run_before_unload=command.run_before_unload,
                    reason=command.reason,
                    timeout=self.timeouts.request_timeout,
                )
                for session in self.sessions
            ]
        )
