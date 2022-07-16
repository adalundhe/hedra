from __future__ import annotations
from typing import Any, List
from hedra.core.pipelines.stages.types.stage_types import StageTypes


class Stage:
    stage_type=StageTypes
    dependencies: List[Stage]=[]
    all_dependencies: List[Stage]=[]
    results: Any = None
    context: Any = None