from .command_library import CommandLibrary


class CommandLibrarian:

    __slots__ = (
        'command_library'
    )

    def __init__(self, page) -> None:
        self.command_library = CommandLibrary(page)

    def get(self, command_name):
        return self.command_library.__getattribute__(command_name)

    