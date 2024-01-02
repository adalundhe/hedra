import asyncio
import math
import psutil
import time
import traceback
from typing import List, Any, Dict, Callable
from .hooks import CallResolver
from .workflow import Workflow


async def cancel_pending(pend: asyncio.Task):
    try:
        if pend.done():
            pend.exception()

            return pend

        pend.cancel()
        await asyncio.sleep(0)
        if not pend.cancelled():
            await pend

        return pend
    
    except asyncio.CancelledError as cancelled_error:
        return cancelled_error

    except asyncio.TimeoutError as timeout_error:
        return timeout_error

    except asyncio.InvalidStateError as invalid_state:
        return invalid_state


class Graph:
    
    def __init__(
        self,
        workflows: List[Workflow],
        context: Dict[
            str, 
            Callable[..., Any] | object
        ] = {}
    ) -> None:
        self.graph = __file__
        self.workflows = workflows
        self.max_active = 0
        self.active = 0
        self._call_optimizer = CallResolver()

        self.context: Dict[
            str, 
            Callable[..., Any] | object
        ] = context

        self._active_waiter: asyncio.Future | None = None

    async def run(self):

        for workflow in self.workflows:
            await self._run(workflow)

    async def setup(self):

        for workflow in self.workflows:
            for hook in workflow.hooks.values():
                self._call_optimizer.add_args(
                    hook.static_args
                )
                
        await self._call_optimizer.optimize_arg_types()
        
        for call_id, args in self._call_optimizer:
            print(call_id, args)

    async def _run(
        self,
        workflow: Workflow
    ):
        
        loop = asyncio.get_event_loop()
        
        completed, pending = await asyncio.wait([
            loop.create_task(
                self._spawn_vu(
                    workflow
                )
            ) async for _ in self._generate(
                workflow
            )
        ], timeout=1)
        
        results: List[List[Any]] = await asyncio.gather(*completed)

        await asyncio.gather(*[
            asyncio.create_task(
                cancel_pending(pend)
            ) for pend in pending
        ])
        
        all_completed: List[Any] = []
        for results_set in results:
            all_completed.extend(results_set)

        return all_completed
        
    async def _generate(self, workflow: Workflow):

        self._active_waiter = asyncio.Future()

        duration = workflow.config.get('duration')
        vus = workflow.config.get('vus')
        threads = workflow.config.get('threads')

        elapsed = 0

        self.max_active = math.ceil(vus * (psutil.cpu_count(logical=False)**2)/threads)

        start = time.monotonic()
        while elapsed < duration:

            remaining = duration - elapsed

            yield remaining

            await asyncio.sleep(0)
            elapsed = time.monotonic() - start

            if self.active > self.max_active:

                remaining = duration - elapsed
                
                try:
                    await asyncio.wait_for(
                        self._active_waiter,
                        timeout=remaining
                    )
                except asyncio.TimeoutError:
                    pass

    async def _spawn_vu(
        self, 
        workflow: Workflow,
    ):

        try:
            results: List[Any] = []

            for hook_set in workflow.traversal_order:

                set_count = len(hook_set)
                self.active += set_count

                results.extend(
                    await asyncio.gather(*[
                        hook() for hook in hook_set 
                    ], return_exceptions=True)
                )

                self.active -= set_count

                if self.active <= self.max_active and self._active_waiter:
                    self._active_waiter.set_result(None)
                    self._active_waiter = asyncio.Future()

        except Exception:
            pass

        return results


            