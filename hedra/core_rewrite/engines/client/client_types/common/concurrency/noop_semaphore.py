"""Synchronization primitives."""

__all__ = ('Lock', 'Event', 'Condition', 'Semaphore',
           'BoundedSemaphore', 'Barrier')

import collections
from asyncio import exceptions
from asyncio import mixins

class _ContextManagerMixin:
    async def __aenter__(self):
        # We have no use for the "as ..."  clause in the with
        # statement for locks.
        return None

    async def __aexit__(self, exc_type, exc, tb):
        pass

class NoOpSemaphore(_ContextManagerMixin, mixins._LoopBoundMixin):
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

    def __init__(self):
        pass