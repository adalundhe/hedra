import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine
from hedra.core.personas.batching import SequenceStep


class SequencedPersonaCollection(DefaultPersona):
    '''
    ---------------------------------------
    Sequenced Persona - (sequence)
    
    Executed actions in batches according to:

    - the actions in the number of batches specified by --batch-count
    - the size specified by --batch-size
    - the order specified by an action's "order" value
    
    waiting between batches for:
    
    - the number of seconds specified by --batch-interval.
    
    Actions are sorted in ascending order prior to execution. 
    When the persona has reached the end of a sequence, it will 
    "wrap" back around and resume iteration from the start of 
    the sequence.
    '''

    def __init__(self, config, handler):
        super(SequencedPersonaCollection, self).__init__(
            config,
            handler
        )

        self.elapsed = 0
        self.no_execution_actions = True

    async def setup(self, actions):
        self.session_logger.debug('Setting up persona...')

        setup_actions = actions.get('setup')
        teardown_actions = actions.get('teardown')
        execution_actions = actions.get('execute')
        await self.engine.create_session(setup_actions)
        self.engine.teardown_actions = teardown_actions

        if execution_actions and len(execution_actions.data) > 0:
            self.actions_count = len(execution_actions.data)
            self.no_execution_actions = False

            async for action in execution_actions:
                parsed_action = await action()
                self._parsed_actions.data.append(parsed_action)


    async def execute(self):
        results = []
        current_action_idx = 0

        self.start = time.time()
        while self.elapsed < self.total_time:
            next_timeout = self.total_time - (time.time() - self.start)
            action = self._parsed_actions[current_action_idx] 

            self.batch.deferred.append(asyncio.create_task(
                action.session.batch_request(
                    action.data,
                    concurrency=self.batch.size,
                    timeout=next_timeout
                )
            ))

            await asyncio.sleep(self.batch.interval.period)

            self.elapsed = time.time() - self.start

            current_action_idx = (current_action_idx + 1) % self.actions_count
        
        self.end = self.elapsed + self.start

        for deferred_batch in self.batch.deferred:
            completed, pending = await deferred_batch
            completed = await asyncio.gather(*completed)
            results.extend(completed)
            
            try:
                for pend in pending:
                    pend.cancel()
            except Exception:
                pass
        
        return results

    async def close(self):
        await self.engine.close()
