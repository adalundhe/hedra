import asyncio
import math
import time
from typing import Any, Callable, Dict, List

import psutil

from .hooks import CallResolver, Hook
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
        context: Dict[str, Callable[..., Any] | object] = {},
    ) -> None:
        self.graph = __file__
        self.workflows = workflows
        self.max_active = 0
        self.active = 0
        self._call_resolver = CallResolver()

        self.context: Dict[str, Callable[..., Any] | object] = context

        self._active_waiter: asyncio.Future | None = None
        self._workflows_by_name: Dict[str, Workflow] = {}

    async def run(self):
        for workflow in self.workflows:
            await self._run(workflow)

    async def setup(self):
        call_ids: List[str] = []
        hooks_by_call_id: Dict[str, Hook] = {}

        for workflow in self.workflows:
            self._workflows_by_name[workflow.name] = workflow

            for hook in workflow.hooks.values():
                self._call_resolver.add_args(hook.static_args)

                hooks_by_call_id.update({call_id: hook for call_id in hook.call_ids})

                call_ids.extend(hook.call_ids)

        await self._call_resolver.resolve_arg_types()

        for call_id, optimized in self._call_resolver:
            print(call_id, optimized)

    async def _run(self, workflow: Workflow):
        loop = asyncio.get_event_loop()

        completed, pending = await asyncio.wait(
            [
                loop.create_task(self._spawn_vu(workflow))
                async for _ in self._generate(workflow)
            ],
            timeout=1,
        )

        results: List[List[Any]] = await asyncio.gather(*completed)

        await asyncio.gather(
            *[asyncio.create_task(cancel_pending(pend)) for pend in pending]
        )

        all_completed: List[Any] = []
        for results_set in results:
            all_completed.extend(results_set)

        return all_completed

    async def _generate(self, workflow: Workflow):
        self._active_waiter = asyncio.Future()

        duration = workflow.config.get("duration")
        vus = workflow.config.get("vus")
        threads = workflow.config.get("threads")

        elapsed = 0

        self.max_active = math.ceil(
            vus * (psutil.cpu_count(logical=False) ** 2) / threads
        )

        start = time.monotonic()
        while elapsed < duration:
            remaining = duration - elapsed

            yield remaining

            await asyncio.sleep(0)
            elapsed = time.monotonic() - start

            if self.active > self.max_active:
                remaining = duration - elapsed

                try:
                    await asyncio.wait_for(self._active_waiter, timeout=remaining)
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
                    await asyncio.gather(
                        *[hook() for hook in hook_set], return_exceptions=True
                    )
                )

                self.active -= set_count

                if self.active <= self.max_active and self._active_waiter:
                    self._active_waiter.set_result(None)
                    self._active_waiter = asyncio.Future()

        except Exception:
            pass

        return results
