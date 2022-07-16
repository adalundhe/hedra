from __future__ import annotations
from typing import List
from hedra.test.stages.types.stage_types import StageTypes


class Stage:
    stage_order=0
    stage_name=None
    stage_type=StageTypes
    config=None
    dependencies: List[Stage]=[]
    all_dependencies: List[Stage]=[]