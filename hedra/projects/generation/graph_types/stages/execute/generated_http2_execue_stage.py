from hedra.core.graphs.stages import (
    Execute,
)

from hedra.core.graphs.hooks import (
    action
)


class ExecuteHTTP2Stage(Execute):

    @action()
    async def http2_get(self):
        return await self.client.http2.get('https://<url_here>')