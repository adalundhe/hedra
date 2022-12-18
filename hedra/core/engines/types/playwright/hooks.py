import asyncio
from typing import Coroutine, List, Generic, TypeVar


A = TypeVar('A')


class Hooks(Generic[A]):

    __slots__ = (
        'before',
        'after',
        'checks',
        'notify',
        'listen',
        'listeners',
        'channels'
    )

    def __init__(self) -> None:
        self.before: Coroutine = None
        self.after: Coroutine = None
        self.checks: List[Coroutine] = []
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

        return names

    def to_serializable(self):
        return {
            'notify': self.notify,
            'listen': self.listen,
            'listeners': [
                listener.name for listener in self.listeners
            ],
            'names': self.to_names()
        }