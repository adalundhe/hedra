import asyncio
from typing import Any, Dict, List, Literal, Optional

from playwright.async_api import Browser, BrowserContext, Geolocation, Page, Playwright

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .browser_page import BrowserPage
from .models.browser import BrowserMetadata


class BrowserSession:

    def __init__(
        self,
        playwright: Playwright,
        pool_size: int,
        timeouts: Timeouts
    ) -> None:
        self.playwright = playwright
        self.pool_size = pool_size
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.pages: asyncio.Queue[BrowserPage] = asyncio.Queue(maxsize=pool_size)
        self.metadata: BrowserMetadata = None
        self.config: Dict[str, Any] = {}
        self.timeouts = timeouts

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
        
        await asyncio.gather(*[
            self.pages.put(
                BrowserPage(
                    asyncio.wait_for(
                        self.context.new_page(),
                        timeout=timeout
                    ),
                    self.timeouts,
                    self.metadata
                )
            ) for _ in range(self.pages)
        ])

    async def next_page(self):
        return await self.pages.get()
    
    async def close(
        self,
        run_before_unload: Optional[bool] = None, 
        reason: Optional[str] = None,
        timeout: Optional[int | float]=None      
    ):
        await asyncio.gather(*[
            self._close(
                self.pages.get_nowait(),
                run_before_unload=run_before_unload,
                reason=reason,
                timeout=timeout
            ) for _ in range(self.pool_size)
        ])

    async def _close(
        self,
        page: BrowserPage,
        run_before_unload: Optional[bool] = None, 
        reason: Optional[str] = None,
        timeout: Optional[int | float]=None  
    ):
        await page.close(
            run_before_unload=run_before_unload,
            reason=reason,
            timeout=timeout
        )
        
    def return_page(
        self,
        page: Page
    ):
        self.pages.put_nowait(page)
