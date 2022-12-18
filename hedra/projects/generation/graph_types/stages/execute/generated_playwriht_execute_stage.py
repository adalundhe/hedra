from hedra.core.graphs.stages import (
    Execute,
)

from hedra.core.graphs.hooks import (
    action
)


class ExecutePlaywrightStage(Execute):

    @action()
    async def open_page(self):
        return await self.client.playwright.goto('https://<url_here>')

    @action()
    async def click_item(self):
        return await self.client.playwright.click('[<selector_here>]')