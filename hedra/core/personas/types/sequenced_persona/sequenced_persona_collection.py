import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine
from hedra.core.personas.batching import SequenceStep
from hedra.core.parsing import ActionsParser


class SequencedPersonaCollection(DefaultPersona):

    def __init__(self, config, handler):
        super(SequencedPersonaCollection, self).__init__(
            config,
            handler
        )

    @classmethod
    def about(cls):
        return '''
        Sequenced Persona - (sequence)

        Executes an ordered sequence of actions where each action represents a "step". Each step is executed as a batch, allowing for easy and logical
        maximization of concurrency. The batch size for each step can be set either using optimization or via the --batch-size argument, and the persona
        will attempt to complete as many actions as possible for a given step over the specified amount of time-per-batch (specified either via optimization 
        or the --batch-time argument). As with other personas, the Sequenced persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, parser: ActionsParser):

        self.session_logger.debug('Setting up persona...')

        self._parsed_actions = await parser.sort_sequence()
        self.actions_count = len(self._parsed_actions)
        self.engine = Engine(self.config, self.handler)

        await self.engine.create_session(parser.setup_actions)
        self.engine.teardown_actions = parser.teardown_actions

        for action_set in parser.actions.values():
            await action_set.setup()

    async def execute(self):
        elapsed = 0
        results = []
        current_action_idx = 0

        await self.start_updates()

        self.start = time.time()

        while elapsed < self.total_time:
            next_timeout = self.total_time - elapsed
            action = self._parsed_actions[current_action_idx]
            
            self.batch.deferred.append(asyncio.create_task(
                action.session.execute_batch(
                    action.parsed,
                    concurrency=self.batch.size,
                    timeout=next_timeout
                )
            ))
            
            await asyncio.sleep(self.batch.interval.period)

            elapsed = time.time() - self.start

            current_action_idx = (current_action_idx + 1) % self.actions_count

        self.end = elapsed + self.start

        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            completed, pending = await deferred_batch
            completed = await asyncio.gather(*completed)
            results.extend(completed)
            
            try:
                for pend in pending:
                    pend.cancel()
            except Exception:
                pass

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results
