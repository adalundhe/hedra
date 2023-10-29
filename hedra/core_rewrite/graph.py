import asyncio
import math
import psutil
import time
import networkx
import traceback
import ast
import inspect
from pprint import pprint
from typing import List, Any, Union, Dict, Callable
from .engines.client.client_types.common.base_action import BaseAction
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

        self.context: Dict[
            str, 
            Callable[..., Any] | object
        ] = context

        self._active_waiter: asyncio.Future | None = None

        for workflow in workflows:
            methods = inspect.getmembers(workflow, inspect.ismethod)
            
            for name, method in methods:
                self.context[name] = method

    async def run(self):

        for workflow in self.workflows:
            results = await self._run(workflow)

    async def setup(
        self,
        workflow: Workflow
    ):
        
        for hook in workflow.hooks.values():
            hook.parser.parser_class = workflow
            hook.parser.parser_class_name = workflow.name

            hook.parser.attributes.update(self.context)

            hook.setup()

            pprint(hook.cache)
               
        sources = []

        for hook_name, hook in workflow.hooks.items():
            workflow.workflow_graph.add_node(hook_name, hook=hook)

        for hook in workflow.hooks.values():

            if len(hook.dependencies) == 0:
                sources.append(hook.name)
            
            for dependency in hook.dependencies:
                workflow.workflow_graph.add_edge(
                    dependency, 
                    hook.name
                )

        for traversal_layer in networkx.bfs_layers(workflow.workflow_graph, sources):
            workflow.traversal_order.append([
                workflow.hooks.get(
                    hook_name
                ).call for hook_name in traversal_layer
            ])

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

        print(len(all_completed))

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
            print(traceback.format_exc())

        return results


            