import asyncio
from typing import Generic, TypeVar
from typing import Coroutine, List, Dict


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
        before: List[List[Coroutine]] = None,
        after: List[List[Coroutine]] = None,
        checks: List[Coroutine] = None
    ) -> None:
        self.before: List[List[Coroutine]] = before
        self.after: List[List[Coroutine]] = after
        self.checks: List[List[Coroutine]] = checks
        self.notify = False
        self.listen = False
        self.channel_events: List[asyncio.Event] = []
        self.listeners: Dict[str, A] = {}
        self.channels: List[Coroutine] = []

    @property
    def channel_hook_names(self):
        if self.channels:
            return [
                channel.name for channel in self.channels
            ]

        return []

    def to_names(self):

        names = {}

        if self.before:
            names['before'] = self.before_hook_name

        if self.after:
            names['after'] = self.after_hook_name

        check_names = []
        for check in self.checks:
            check_names.append(check.name)

        names['checks'] = check_names
        names['channels'] = self.channel_hook_names

        names['listeners'] = [
            listener_name for listener_name in self.listeners
        ]

        return names

    def to_serializable(self):
        return {
            'notify': self.notify,
            'listen': self.listen,
            'names': self.to_names()
        }
