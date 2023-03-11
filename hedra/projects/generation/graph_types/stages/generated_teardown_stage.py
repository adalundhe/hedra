from hedra.core.graphs.stages import Teardown
from hedra.core.graphs.hooks import teardown


class TeardownStage(Teardown):

    @teardown()
    async def teardown_previous_stage(self):
        pass