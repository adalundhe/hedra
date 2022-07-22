from __future__ import annotations
import asyncio
from typing import Any, List
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


class Stage:
    stage_type=StageTypes
    dependencies: List[Stage]=[]
    all_dependencies: List[Stage]=[]
    next_context: Any = None
    context: Any = None

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        self.state = StageStates.INITIALIZED
        self.next_stage: str = None