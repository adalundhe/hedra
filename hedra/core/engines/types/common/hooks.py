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

    @property
    def before_hook_name(self):
        if self.before:
            return self.before.name

    @property
    def after_hook_name(self):
        if self.after:
            return self.after.name

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
            listener.name for listener in self.listeners
        ]

        return names

    def to_serializable(self):
        return {
            'notify': self.notify,
            'listen': self.listen,
            'names': self.to_names()
        }