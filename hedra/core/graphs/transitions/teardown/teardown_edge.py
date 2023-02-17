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

        self.requires = [
            'setup_stage_ready_stages',
            'setup_stage_candidates',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'execute_stage_setup_hooks',
            'execute_stage_results'
        ]

        self.provides = [
            'execute_stage_results'
        ]

    async def transition(self):
        self.source.state = StageStates.TEARDOWN_INITIALIZED

        history = self.history[(self.from_stage_name, self.source.name)]

        await self.source.context.update(history)

        if self.timeout:
            await asyncio.wait_for(self.timeout.run(), timeout=self.timeout)

        else:
            await self.source.run()

        for provided in self.provides:
            history[provided] = self.source.context[provided]

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self._update(self.destination)

        self.source.state = StageStates.TEARDOWN_COMPLETE

        return None, self.destination.stage_type

    def _update(self, destination: Stage):

        for edge_name in self.history:

            history = self.history[edge_name]

            if self.next_history.get(edge_name) is None:
                self.next_history[edge_name] = {}
                
            self.next_history[edge_name].update({
                key: value for key, value  in history.items() if key in self.provides
            })


        if self.next_history.get((self.source.name, destination.name)) is None:
            self.next_history[(self.source.name, destination.name)] = {}

        self.next_history.update({
            (self.source.name, destination.name): {}
        })

    def split(self) -> None:
        pass
