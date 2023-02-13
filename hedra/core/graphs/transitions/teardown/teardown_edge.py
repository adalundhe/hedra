import asyncio
from typing import Dict, Any
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.teardown.teardown import Teardown
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes



class TeardownEdge(BaseEdge[Teardown]):

    def __init__(self, source: Teardown, destination: Stage) -> None:
        super(
            TeardownEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):
        self.source.state = StageStates.TEARDOWN_INITIALIZED

        if self.timeout:
            await asyncio.wait_for(self.timeout.run(), timeout=self.timeout)

        else:
            await self.source.run()

        self.source.state = StageStates.TEARDOWN_COMPLETE

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        self.next_history.update({
            (self.source.name, destination.name): {}
        })

    def split(self) -> None:
        pass
