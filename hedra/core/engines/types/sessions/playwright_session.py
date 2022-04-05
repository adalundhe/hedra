
from typing import List
from playwright.async_api import async_playwright
from async_tools.datatypes import AsyncDict
from hedra.core.engines.types.helpers.playwright_helpers import CommandLibrarian

class PlaywrightSession:

    def __init__(self, browser_type=None, device_type=None, locale=None, geolocations=None, permissions=None, color_scheme=None, batch_size=None) -> None:
        self.browser_type = browser_type
        self.device_type = device_type
        self.locale = locale
        self.geolocations = geolocations
        self.permissios = permissions
        self.color_scheme = color_scheme
        self.batch_size = batch_size

    @classmethod
    def about_librarian(cls):
        return CommandLibrarian.about()

    @classmethod
    def about_library(cls):
        return CommandLibrarian.about_library()

    @classmethod
    def about_registered_commands(cls):
        return CommandLibrarian.about_registered()

    @classmethod
    def about_command(cls, command):
        return CommandLibrarian.about_command(command)

    async def create(self) -> List[CommandLibrarian]:

        librarians = []

        playwright = await async_playwright().start()

        if self.browser_type == "safari" or self.browser_type == "webkit":
            self.browser = await playwright.webkit.launch()

        elif self.browser_type == "firefox":
            self.browser = await playwright.firefox.launch()

        else:
            self.browser = await playwright.chromium.launch()


        config = AsyncDict()

        if self.device_type:
            device = playwright.devices[self.device_type]

            config = {
                **device
            }

        if self.locale:
            config['locale'] = self.locale

        if self.geolocations:
            config['geolocation'] = self.geolocations

        if self.permissios:
            config['permissions'] = self.permissios

        if self.color_scheme:
            config['color_scheme'] = self.color_scheme


        has_options = await config.size() > 0

        for _ in range(self.batch_size):

            if has_options:        
                context = await self.browser.new_context(
                    **config.data
                )
    
            else:
                context = await self.browser.new_context()
                

            page = await context.new_page()
            command_librarian = CommandLibrarian(page)

            await librarians.append(command_librarian)

        return librarians