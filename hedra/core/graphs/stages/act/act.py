from typing import Generic
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.client import Client
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.types.stage_types import StageTypes
from typing import Optional


T = TypeVarTuple('T')


class Act(Stage, Generic[Unpack[T]]):
    stage_type=StageTypes.ACT
    priority: Optional[str]=None

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client: Client[Unpack[T]] = Client(
            self.graph_name,
            self.graph_id,
            self.name,
            self.stage_id
        )
        
        self.accepted_hook_types = [ 
            HookType.ACTION,
            HookType.CHANNEL, 
            HookType.CHECK,
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT,
            HookType.LOAD,
            HookType.SAVE,
            HookType.TASK,
            HookType.TRANSFORM
        ]

        self.priority = self.priority
        if self.priority is None:
            self.priority = 'auto'

        self.priority_level: StagePriority = StagePriority.map(
            self.priority
        )

    @Internal()
    async def run(self):
        await self.setup_events()
        self.dispatcher.assemble_execution_graph()
        await self.dispatcher.dispatch_events(self.name)