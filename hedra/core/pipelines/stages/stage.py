from __future__ import annotations
from typing import Any, Dict, List
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.engines.client.time_parser import TimeParser
from .parallel.batch_executor import BatchExecutor


class Stage:
    stage_type=StageTypes
    dependencies: List[Stage]=[]
    all_dependencies: List[Stage]=[]
    next_context: Any = None
    context: Any = None
    stage_timeout=None

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        self.state = StageStates.INITIALIZED
        self.next_stage: str = None
        self.hooks: Dict[HookType, List[Hook]] = {}
        self.accepted_hook_types = []
        self.workers: int = None
        self.worker_id: int = 1
        self.generation_stage_names = []
        self.generation_id = 1
        self._shutdown_task = None
        self.requires_shutdown = False
        self.allow_parallel = False
        self.executor: BatchExecutor = None

        if self.stage_timeout:
            time_parser = TimeParser(self.stage_timeout)

            self.timeout = time_parser.time
        
        else:
            self.timeout = None

    @Internal
    async def run(self):
        pass