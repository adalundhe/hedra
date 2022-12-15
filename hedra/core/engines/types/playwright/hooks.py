from typing import Coroutine, List


class Hooks:

    def __init__(self) -> None:
        self.before: Coroutine = None
        self.after: Coroutine = None
        self.checks: List[Coroutine] = []

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