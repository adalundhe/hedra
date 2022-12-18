from hedra.core.graphs.stages import (
    Execute,
)

from hedra.core.graphs.hooks import (
    action
)


class ExecuteWebsocketStage(Execute):

    @action()
    async def webbsocket_send(self):
        return await self.client.websocket.send('https://<url_here>', data={"PING": "PONG"})

    @action()
    async def webbsocket_listen(self):
        return await self.client.websocket.listen('https://<url_here>')