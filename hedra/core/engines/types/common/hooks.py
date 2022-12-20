import asyncio
from typing import Generic, TypeVar, Union
from typing import Coroutine, List

A = TypeVar('A')


class Hooks(Generic[A]):

    __slots__ = (
        'before',
        'after',
        'checks',
        'notify',
        'listen',
        'channel_events',
        'listeners',
        'channels'
    )

    def __init__(
        self,
        before: Coroutine = None,
        after: Coroutine = None,
        checks: List[Coroutine] = []
    ) -> None:
        self.before: Coroutine = before
        self.after: Coroutine = after
        self.checks: List[Union[str, Coroutine]] = checks
        self.notify = False
        self.listen = False
        self.channel_events: List[asyncio.Event] = []
        self.listeners: List[A] = []
        self.channels: List[Coroutine] = []

    def to_names(self):

        names = {}

        if self.before:
            names['before'] = self.before.__qualname__

        if self.after:
            names['after'] = self.after.__qualname__

        check_names = []
        for check in self.checks:
            check_names.append(check.__qualname__)

        names['checks'] = check_names
        names['channels'] = [
            channel.__qualname__ for channel in self.channels
        ]

        names['listeners'] = [
            listener.name for listener in self.listeners
        ]

        return names

    def to_serializable(self):
        return {
            'notify': self.notify,
            'listen': self.listen,
            'names': self.to_names()
        }