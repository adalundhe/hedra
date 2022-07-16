import time
import asyncio
from typing import List
import psutil
from async_tools.functions.awaitable import awaitable

from hedra.core.personas.batching.batch import Batch
from hedra.test.hooks.hook import Hook
from easy_logger import Logger
from hedra.core.personas.batching import Batch
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.engines import Engine
from hedra.core.personas.utils import parse_time
from hedra.core.parsing import ActionsParser
from hedra.test.hooks.types import HookType


class DefaultPersona:

    def __init__(self, config, handler):
        self.config = config.executor_config
        self.actions = []
        self._hooks: List[Hook] = []
        self.handler = handler
        self.engine = Engine(self.config, self.handler)
        self.batch = Batch(self.config)
        self._live_updates = config.distributed_config.get('live_progress_updates', False)

        self.is_parallel = config.runner_mode.find('parallel') > -1
        self.pool_size = self.config.get('pool_size')
        self.results = []
        self.stats = {}

        if self.is_parallel and self.pool_size is None:
            self.pool_size = psutil.cpu_count(logical=False)

        self.total_time = parse_time(
            self.config.get('total_time', 60)
        )

        self.duration = 0
        self.total_actions = 0
        self.total_elapsed = 0
        self.start = 0
        self.end = 0
        self.completed_actions = 0
        self.completed_time = 0
        self.run_timer = False
        self.actions_count = 0

        self.is_timed = True
        self.timer_thread = None

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.loop = None
        self.current_action_idx = 0

    @classmethod
    def about(cls):
        return '''
        Default Persona - (default)

        Executes as many actions as possible of the specified batch size (can be set either using optimization or via the --batch-size argument) for the
        specified amount of time-per-batch (specified either via optimization or the --batch-time argument) for the total amount of time specified by 
        the --total-time argument.
        '''

    @classmethod
    def about_batches(cls):
        return Batch.about()

    @classmethod
    def about_batch_intervals(cls):
        return BatchInterval.about()

    async def setup(self, parser: ActionsParser):
        self.session_logger.debug('Setting up persona...')
        
        self.actions = parser.actions

        for action_set in parser.action_sets.values():
            self.actions_count += len(action_set.actions)
            self._hooks.extend(action_set.actions)
            
            setup_hooks = action_set.hooks.get(HookType.SETUP)
            for setup_hook in setup_hooks:
                await setup_hook.call(action_set)

    async def execute(self):
        hooks = self._hooks
        total_time = self.total_time

        await self.start_updates()
        
        start = time.time()

        completed, pending = await asyncio.wait([
            asyncio.create_task(
                hooks[action_idx].session.execute_prepared_request(
                    hooks[action_idx].action
                )
            ) async for action_idx in self.generator(total_time)
        ], timeout=1)

        self.end = time.time()
        
        results = await asyncio.gather(*completed, return_exceptions=True)
        

        await self.stop_updates()

        self.start = start
        for pend in pending:
            try:
                pend.cancel()
            except Exception:
                pass

        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return results

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%self.batch.size == 0:
                action_idx = (action_idx + 1)%self.actions_count

    async def close(self):
        await self.engine.close()

    async def start_updates(self):
        if self._live_updates:
            self.timer_thread = asyncio.create_task(self._run_timer_in_background())
    
    async def stop_updates(self):
        if self._live_updates:
            self.run_timer = False
            await self.timer_thread

    def run_updates_in_background(self):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._run_timer_in_background())

    async def _run_timer_in_background(self):
        start = time.time()
        self.run_timer = True

        while self.run_timer:
            completed_actions = 0
            self.completed_time = time.time() - start

            for deferred_batch in self.batch.deferred:
                completed_actions += await awaitable(len, deferred_batch)


            self.completed_actions = completed_actions
            await asyncio.sleep(1)

        completed_actions = 0
        for deferred_batch in self.batch.deferred:
            completed_actions += await awaitable(len, deferred_batch)

        self.completed_actions = completed_actions
        self.completed_time = time.time() - start
        