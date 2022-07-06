from typing import Iterator
from typing import Union
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.playwright import Command

from hedra.test.actions.base import Action  



class Registry:

    def __init__(self) -> None:
        self.data = {}
        self.count = 0

    def __iter__(self) -> Iterator[Action]:
        for action in self.data.values():
            yield action

    def __getitem__(self, name: str) -> Union[Request, Command]:
        return self.data.get(name)

    def __setitem__(self, name: str, value: Union[Request, Command]):
        self.data[name] = value
        self.count = len(self.data)

    def to_list(self):
        return list(self.data.values())


registered = {}