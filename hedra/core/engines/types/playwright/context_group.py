
import asyncio
import time
from async_tools.datatypes import AsyncList
from typing import List
from playwright.async_api import async_playwright
from .command import Command
from .result import PlaywrightResult
from .command_librarian import CommandLibrarian

class ContextGroup:

    def __init__(self, browser_type=None, device_type=None, locale=None, geolocations=None, permissions=None, color_scheme=None, concurrency=None) -> None:
        self.browser_type = browser_type
        self.device_type = device_type
        self.locale = locale
        self.geolocations = geolocations
        self.permissions = permissions
        self.color_scheme = color_scheme
        self.concurrency = concurrency
        self.librarians: List[CommandLibrarian] = []
        self.config = {}
        self.contexts = []
        self.sem = asyncio.Semaphore(concurrency)

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
                **device
            }

        if self.locale:
            self.config['locale'] = self.locale

        if self.geolocations:
            self.config['geolocation'] = self.geolocations

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

    async def execute(self, command: Command):
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

    async def execute_batch(self, command: Command, timeout: float=None):
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
