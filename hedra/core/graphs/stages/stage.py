from __future__ import annotations
import threading
import os
from typing import Any, Dict, List, Union
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.engines.client.time_parser import TimeParser
from hedra.logging import HedraLogger
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.reporter.reporter_plugin import ReporterPlugin
from hedra.plugins.types.plugin_types import PluginType
from .parallel.batch_executor import BatchExecutor


class Stage:
    stage_type=StageTypes
    dependencies: List[Stage]=[]
    all_dependencies: List[Stage]=[]
    next_context: Any = None
    context: Any = None
    stage_timeout=None
    plugins: Dict[str, Union[EnginePlugin, ReporterPlugin]] = {}

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
        self.plugins_by_type: Dict[PluginType, Dict[str, Union[EnginePlugin, ReporterPlugin]]] = {}

        self.logger: HedraLogger = HedraLogger()
        self.logger.initialize()

        self.graph_name: str = None
        self.graph_id: str = None

        if self.stage_timeout:
            time_parser = TimeParser(self.stage_timeout)

            self.timeout = time_parser.time
        
        else:
            self.timeout = None

    @Internal
    async def run(self):
        pass

    @property
    def thread_id(self) -> int:
        return threading.current_thread().ident

    @property
    def process_id(self) -> int:
        return os.getpid()

    @property
    def metadata_string(self):
        return f'Graph - {self.graph_name}:{self.graph_id} - thread:{self.thread_id} - process:{self.process_id} - Stage: {self.name} - '
