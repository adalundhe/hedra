import psutil
import asyncio
import uvloop
import math
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from async_tools.functions import check_event_loop
from async_tools.datatypes.async_list import AsyncList
from easy_logger import Logger
from async_tools.functions import awaitable
from hedra.core.personas.utils import parse_time
from hedra.core.pipelines.stages import setup
from .stages import (
    Execution,
    Optimize,
    Warmup,
    Setup,
    Results
)


class Pipeline:

    stages = {
        'execute': Execution,
        'optimize': Optimize,
        'warmup': Warmup,
        'setup': Setup,
        'results': Results
    }

    def __init__(self, config) -> None:

        logger = Logger()
        self.session_logger = logger.generate_logger()
        
        self._is_parallel = config.runner_mode.find('parallel') > -1
        self._no_run_visuals = config.executor_config.get('no_run_visuals', False)
        self.pool_size = config.executor_config.get('pool_size')

        if self._is_parallel and self.pool_size is None:
            self.pool_size = psutil.cpu_count(logical=False)

        self.config = config
        self.selected_persona = None
        self._selected_stages = AsyncList()
        self.results = []
        self.stats = {}

        check_event_loop(self.session_logger)

        try:
            self._event_loop = asyncio.get_running_loop()

        except Exception:          
            self._event_loop = asyncio.get_event_loop()

    @classmethod
    def about(cls):

        pipeline_stages = '\n\t'.join([f'- {stage_name}' for stage_name in cls.stages.keys()])

        return f'''
        Pipelines

        key-arguments:

        --warmup <warmup_duration_seconds>

        --optimize <optimization_iterations>

        Pipelines organize execution of performance tests into discrete, recognizable stages. This allows personas
        to focus on action execution organization as opposed to overall test session managment. The following stages
        are currently supported:

        {pipeline_stages}

        For more information on each stage, run the command:

            hedra --about pipeline:<stage_name>


        Related Topics

        - personas
        - runners

        '''

    def  __iter__(self):
        for stage in self._selected_stages.data:
            yield stage

    async def initialize(self, persona, actions):
        self.selected_persona = persona.selected_persona
        for stage in self.stages:
            initialized_stage = self.stages[stage](self.config, self.selected_persona)
            if initialized_stage.execute_stage:
                initialized_stage.actions = actions
                await self._selected_stages.append(initialized_stage)

        self._selected_stages = await self._selected_stages.sort(key=lambda stage: stage.order)

    async def execute(self):
        async for stage in self._selected_stages:
            self.selected_persona = await stage.execute(
                self.selected_persona
            )

    async def execute_stage(self, stage):
        self.selected_persona = await stage.execute(self.selected_persona)

    async def get_results(self):
        results_stage = self._selected_stages.data[len(self._selected_stages.data)-1]
        self.results = results_stage.results
        self.stats = results_stage.stats
        return self.results, self.stats

    async def get_completed_count(self):
        if self.selected_persona:
            return self.selected_persona.completed_actions, int(self.selected_persona.completed_time)

        else:
            return 0, 0
        
