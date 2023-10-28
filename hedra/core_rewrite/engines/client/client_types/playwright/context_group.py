
import asyncio
import time
from hedra.tools.data_structures import AsyncList
from typing import List, Dict, Any
from .command import PlaywrightCommand
from .result import PlaywrightResult
from .command_librarian import CommandLibrarian


try:
    
    from playwright.async_api import async_playwright
    from playwright.async_api import Geolocation

except Exception:
    async_playwright = lambda: None
    Geolocation = None

class ContextGroup:

    __slots__ = (
        'browser_type',
        'device_type',
        'locale',
        'geolocation',
        'permissions',
        'color_scheme',
        'concurrency',
        'librarians',
        'config',
        'options',
        'contexts',
        'sem'
    )

    def __init__(
        self, 
        browser_type: str=None, 
        device_type: str=None, 
        locale: str=None, 
        geolocation: Geolocation=None, 
        permissions: List[str]=None, 
        color_scheme: str=None, 
        concurrency: int=None,
        options: Dict[str, Any]=None
    ) -> None:
        self.browser_type = browser_type
        self.device_type = device_type
        self.locale = locale
        self.geolocation = geolocation
        self.permissions = permissions
        self.color_scheme = color_scheme
        self.concurrency = concurrency
        self.librarians: List[CommandLibrarian] = []
        self.config = {}
        self.options = options
        self.contexts = []
        self.sem = asyncio.Semaphore(value=concurrency)

    async def create(self) -> None:

        playwright = await async_playwright().start()

        if self.browser_type == "safari" or self.browser_type == "webkit":
            self.browser = await playwright.webkit.launch()

        elif self.browser_type == "firefox":
            self.browser = await playwright.firefox.launch()

        else:
            self.browser = await playwright.chromium.launch()


        self.config = {}

        if self.device_type:
            device = playwright.devices[self.device_type]

            self.config = {
                **device,
                **self.options
            }

        if self.locale:
            self.config['locale'] = self.locale

        if self.geolocation:
            self.config['geolocation']: Geolocation = self.geolocation

        if self.permissions:
            self.config['permissions'] = self.permissions

        if self.color_scheme:
            self.config['color_scheme'] = self.color_scheme


        has_options = len(self.config) > 0

        for _ in range(self.concurrency):

            if has_options:        
                context = await self.browser.new_context(
                    **self.config
                )
    
            else:
                context = await self.browser.new_context()
                
            self.contexts.append(context)
            page = await context.new_page()
            command_librarian = CommandLibrarian(page)

            self.librarians.append(command_librarian)

    async def execute(self, command: PlaywrightCommand):
        result = PlaywrightResult(command)
        await self.sem.acquire()
        start = 0
        librarian = self.librarians.pop()

        try:
            playwright_command = librarian.get(command.command)

            start = time.time()

            result.data = await playwright_command(command)

            elapsed = time.time() - start

            result.time = elapsed

            self.librarians.append(librarian)
            self.sem.release()

            return result

        except Exception as e:
            elapsed = time.time() - start
            result.time = elapsed
            result.error = e

            self.librarians.append(librarian)
            self.sem.release()

            return result

    async def execute_batch(self, command: PlaywrightCommand, timeout: float=None):
        return await asyncio.wait([
            self.execute(command) async for _ in AsyncList(range(self.concurrency))
        ], timeout=timeout)

    async def close(self):
        try:
            for context in self.contexts:
                await context.close()
                
            await self.browser.close()
        
        except Exception:
            pass
