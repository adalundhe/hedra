import threading
import time
import asyncio
import traceback
from typing import List, Tuple
from urllib.request import Request
import psutil
import uvloop
from async_tools.functions.awaitable import awaitable

from hedra.core.personas.batching.batch import Batch
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from async_tools.datatypes.async_list import AsyncList
from easy_logger import Logger
from hedra.core.personas.batching import Batch
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.engines import Engine
from hedra.core.personas.utils import parse_time


class DefaultPersona:

    def __init__(self, config, handler):
        self.config = config.executor_config
        self.actions = AsyncList()
        self._parsed_actions = AsyncList()
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

    async def setup(self, actions):
        self.session_logger.debug('Setting up persona...')

        self._parsed_actions = await actions.to_async_list()
        self.actions_count = await self._parsed_actions.size()

        await self.engine.setup(AsyncList(actions.parser.setup_actions))  
        await self.engine.set_teardown_actions(actions.parser.teardown_actions)

        self.engine = self.engine.engine

        self.duration = self.total_time

    async def load_batches(self):
        # self.actions = AsyncList()
        # for idx in range(self.batch.size):
        #     action_idx = idx % self.actions_count
        #     action = self._parsed_actions[action_idx]
        #     await self.actions.append(action)
        pass

    async def execute(self):

        elapsed = 0
        results = []

        await self.start_updates()
        
        self.start = time.time()

        current_action_idx = 0

        while elapsed < self.duration:
            next_timeout = self.duration - (time.time() - self.start)   
            
            self.batch.deferred.append(asyncio.create_task(
                self._parsed_actions[current_action_idx].execute(next_timeout)
            ))
            
            await asyncio.sleep(self.batch.time)
            elapsed = time.time() - self.start

            current_action_idx = (current_action_idx + 1) % self.actions_count

        self.end = elapsed + self.start
        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            batch, pending = await deferred_batch
            collected = await asyncio.gather(*batch, return_exceptions=True)
            results.extend(collected)
            
            try:
                for pend in pending:
                    pend.cancel()
            except Exception as e:
                pass
            
            
        self.total_actions = len(results)
        self.total_elapsed = elapsed
        self.optimized_params = None

        return results

    async def _execute_batch(self):
        next_timeout = self.duration - (time.time() - self.start)    
        return await asyncio.wait(
            [ action async for action in self.engine.defer_all(self.actions)], 
            timeout=next_timeout if next_timeout > 0 else 1
        )

    async def close(self):
        await self.engine.close()

    async def start_updates(self):
        if self._live_updates:
            self.loop = asyncio.get_running_loop()

            self.timer_thread = threading.Thread(target=self.run_updates_in_background)
            await self.loop.run_in_executor(None, self.timer_thread.start)
    
    async def stop_updates(self):
        if self._live_updates:
            self.run_timer = False
            await self.loop.run_in_executor(None, self.timer_thread.join)
            self.completed_actions

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
        