import functools
import inspect
from hedra.test.hooks.hook import Hook
from hedra.test.registry.registrar import registar
from typing import Dict, List

# from hedra.core.engines.types.common.context import Context
from hedra.test.hooks.types import HookType
# from hedra.core.engines.types.common.hooks import Hooks
from hedra.test.client import Client
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage



class Execute(Stage):
    stage_type=StageTypes.EXECUTE
    client: Client = None