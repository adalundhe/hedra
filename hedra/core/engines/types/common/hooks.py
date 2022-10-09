from typing import Coroutine, List



class Hooks:

    __slots__ = (
        'before',
        'after',
        'checks'
    )

    def __init__(
        self,
        before: Coroutine = None,
        after: Coroutine = None,
        checks: List[Coroutine] = []
    ) -> None:
        self.before = before
        self.after = after
        self.checks = checks

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