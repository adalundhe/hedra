from __future__ import annotations
import threading
import os
import uuid
from collections import defaultdict
from typing import Any, Dict, List
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.graphs.events.base_event import BaseEvent
from hedra.core.graphs.events.event_dispatch import EventDispatcher
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.stages.base.parallel.batch_executor import BatchExecutor
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.simple_context import SimpleContext

from hedra.logging import HedraLogger
from hedra.plugins.types.common.plugin import Plugin
from hedra.plugins.types.plugin_types import PluginType



class Stage:
    stage_type=StageTypes
    dependencies: List[Stage]=[]
    all_dependencies: List[Stage]=[]
    next_context: Any = None
    stage_timeout=None
    plugins: Dict[str, Plugin] = {}

    def __init__(self) -> None:
        self.name = self.__class__.__name__
        self.graph_path: str = None
        self.stage_id = str(uuid.uuid4())

        self.state = StageStates.INITIALIZED
        self.next_stage: str = None
        self.hooks: Dict[HookType, List[Hook]] = defaultdict(list)
        self.accepted_hook_types: List[HookType] = []
        self.workers: int = None
        self.worker_id: int = 1
        self.generation_stage_names = []
        self.generation_id = 1
        self.requires_shutdown = False
        self.allow_parallel = False
        self.executor: BatchExecutor = None
        self.plugins_by_type: Dict[PluginType, Dict[str, Plugin]] = defaultdict(dict)

        self.core_config = {}
        self.context = SimpleContext()
        self.logger: HedraLogger = HedraLogger()
        self.logger.initialize()

        self.graph_name: str = None
        self.graph_id: str = None

        if self.stage_timeout:
            time_parser = TimeParser(self.stage_timeout)

            self.timeout = time_parser.time
        
        else:
            self.timeout = None

        self.internal_hooks = ['run']
        self.dispatcher = EventDispatcher()
        self.linked_events = defaultdict(list)

    @Internal()
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
        return f'Graph - {self.graph_name}:{self.graph_id} - thread:{self.thread_id} - process:{self.process_id} - Stage: {self.name}:{self.stage_id} - '
    
    @Internal()
    async def setup_events(self):

        for event in self.dispatcher.events_by_name.values():
            event.context.update(self.context)
            
            if event.source.context:
                event.source.context.update(self.context)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing events')
