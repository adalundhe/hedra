import asyncio
from collections import deque
from typing import Any, Deque, Dict, List, Literal, Optional

from playwright.async_api import Browser, BrowserContext, Geolocation, Page, Playwright

from .models.browser import BrowserMetadata


class BrowserSession:

    def __init__(
        self,
        playwright: Playwright,
        pages: int
    ) -> None:
        self.playwright = playwright
        self.pages = pages
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.pages: Deque[Page] = deque()
        self.metadata: BrowserMetadata = None
        self.config: Dict[str, Any] = {}

        self.sem = asyncio.Semaphore(pages)

    async def open(
        self,
        browser_type: Optional[
            Literal[
                'safari',
                'webkit',
                'firefox',
                'chrome',
                'chromium'
            ]
        ]=None,
        device_type: Optional[str]=None, 
        locale: Optional[str]=None, 
        geolocation: Optional[Geolocation]=None, 
        permissions: Optional[List[str]]=None, 
        color_scheme: Optional[str]=None, 
        options: Dict[str, Any]={},
        timeout: int | float=60
    ):
        
        self.metadata = BrowserMetadata(
            browser_type=browser_type,
            device_type=device_type,
            locale=locale,
            geolocation=geolocation,
            permissions=permissions,
            color_scheme=color_scheme
        )

        match self.metadata.browser_type:
            case 'safari' | 'webkit':
                self.browser = await self.playwright.webkit.launch()

            case 'firefox':
                self.browser = await self.playwright.firefox.launch()

            case 'chrome' | 'chromium':
                self.browser = await self.playwright.chromium.launch()

            case _:
                self.browser = await self.playwright.chromium.launch()

        if self.metadata.device_type:
            device = self.playwright.devices[self.metadata.device_type]

            self.config = {
                **device,
                **options
            }

        if self.metadata.locale:
            self.config['locale'] = locale

        if self.metadata.geolocation:
            self.config['geolocation'] = geolocation

        if self.metadata.permissions:
            self.config['permissions'] = permissions

        if self.metadata.color_scheme:
            self.config['color_scheme'] = color_scheme


        has_options = len(self.config) > 0

        if has_options:        
            self.context = await asyncio.wait_for(
                self.browser.new_context(
                    **self.config
                ),
                timeout=timeout
            )

        else:
            self.context = await asyncio.wait_for(
                self.browser.new_context(),
                timeout=timeout
            )

        self.pages.extend(
            await asyncio.gather(*[
                asyncio.wait_for(
                    self.context.new_page(),
                    timeout=timeout
                ) for _ in range(self.pages)
            ])
        )

    async def next_page(self):
        await self.sem.acquire()
        return self.pages.popleft()
    
    def return_page(
        self,
        page: Page
    ):
        self.pages.append(page)
        self.sem.release()