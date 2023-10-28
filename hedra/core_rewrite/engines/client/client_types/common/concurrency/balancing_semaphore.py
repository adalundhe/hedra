"""Synchronization primitives."""

__all__ = ('Lock', 'Event', 'Condition', 'Semaphore',
           'BoundedSemaphore', 'Barrier')

import collections
from asyncio import exceptions
from asyncio import mixins

class _ContextManagerMixin:
    async def __aenter__(self):
        await self.acquire()
        # We have no use for the "as ..."  clause in the with
        # statement for locks.
        return None

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

class BalancingSemaphore(_ContextManagerMixin, mixins._LoopBoundMixin):

    __slots__ = (
        '_value',
        '_slots',
        '_waiters',
        '_busy',
        '_wakeup_scheduled'
    )

    """A Semaphore implementation.
    A semaphore manages an internal counter which is decremented by each
    acquire() call and incremented by each release() call. The counter
    can never go below zero; when acquire() finds that it is zero, it blocks,
    waiting until some other thread calls release().
    Semaphores also support the context management protocol.
    The optional argument gives the initial value for the internal
    counter; it defaults to 1. If the value given is less than 0,
    ValueError is raised.
    """

    def __init__(self, value=1):
        if value < 0:
            raise ValueError("Semaphore initial value must be >= 0")
        self._value = value
        self._slots = [idx for idx in range(value)]
        self._waiters = tuple([collections.deque() for _ in self._slots])
        self._busy = bytearray([0 for _ in self._slots])
        self._wakeup_scheduled = False

    def __repr__(self):
        res = super().__repr__()
        extra = 'locked' if self.locked() else f'unlocked, value:{self._value}'
        if self._waiters:
            extra = f'{extra}, waiters:{len(self._waiters)}'
        return f'<{res[1:-1]} [{extra}]>'

    def _wake_up_next(self, min_idx: int):
        while self._waiters[min_idx]:
            waiter = self._waiters[min_idx].popleft()
            if not waiter.done():
                waiter.set_result(None)
                self._wakeup_scheduled = True
                return

    def locked(self):
        """Returns True if semaphore can not be acquired immediately."""
        return self._value == 0

    async def acquire(self):
        """Acquire a semaphore.
        If the internal counter is larger than zero on entry,
        decrement it by one and return True immediately.  If it is
        zero on entry, block, waiting until some other coroutine has
        called release() to make it larger than 0, and then return
        True.
        """
        # _wakeup_scheduled is set if *another* task is scheduled to wakeup
        # but its acquire() is not resumed yet

        # Find the line with the smalles number of futures waiting. Assign
            # to that queue
        min_queue = min(self._busy)
        min_idx = self._busy.index(min_queue)

        # while the line of the minimum is not empty.
        while self._wakeup_scheduled or self._busy[min_idx] > 0:
            fut = self._get_loop().create_future()
            self._waiters[min_idx].append(fut)

            try:
                await fut

                self._wakeup_scheduled = False
            except exceptions.CancelledError:
                self._wake_up_next(min_idx)
                raise
        
        # A line now has waiters.
        self._busy[min_idx] += 1

        return True

    def release(self):
        """Release a semaphore, incrementing the internal counter by one.
        When it was zero on entry and another coroutine is waiting for it to
        become larger than zero again, wake up that coroutine.
        """
        max_queue = max(self._busy)
        max_idx = self._busy.index(max_queue)

        self._busy[max_idx] -= 1
        self._wake_up_next(max_idx)