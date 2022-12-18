from hedra.core.graphs.stages import (
    Execute,
)

from hedra.core.graphs.hooks import (
    action
)


class ExecuteGraphQLHttp2Stage(Execute):

    @action()
    async def http2_query(self):
        return await self.client.graphqlh2.query(
            """
            query <query_name> {
                ...
            }
            """
        )