from typing import Generic
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.client import Client
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes


T = TypeVarTuple('T')


class Act(Stage, Generic[Unpack[T]]):
    stage_type=StageTypes.ACT

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


    @Internal()
    async def run(self):
        await self.setup_events()
        await self.dispatcher.dispatch_events(self.name)