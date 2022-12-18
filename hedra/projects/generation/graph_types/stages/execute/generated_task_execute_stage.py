from hedra.core.graphs.stages import (
    Execute,
)

from hedra.core.graphs.hooks import (
    task
)


class ExecuteTaskStage(Execute):

    counter=0

    @task()
    async def task_http_get(self):

        response = await self.client.http.get('https://<url_here>')

        if response.status >= 200 and response.status < 300:
            self.counter += 1

        return response