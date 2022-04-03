from .command_library import CommandLibrary


class CommandLibrarian:


    def __init__(self, page) -> None:
        self.command_library = CommandLibrary(page)

    @classmethod
    def about(cls):
        return '''
        Playwright Command Librarian

        The Playwright command librarian is a helper class designed to assist in quick lookup
        of Playwright commands for incoming actions from the Playwright Engine. The librarian
        uses the action's type attribute to lookup the command from the Playwright Command Library,
        raising an AttributeNotFound exception if the command does not exist.

        For more information on the Playwright Command Library, run the command:

            hedra:engine:playwright:library

        '''

    @classmethod
    def about_library(cls):
        return CommandLibrary.about()

    @classmethod
    def about_registered(cls):
        return CommandLibrary.about_registered()

    @classmethod
    def about_command(cls, command):
        return CommandLibrary.registered.get(command)

    async def lookup(self, action):
        yield self.command_library.__getattribute__(action.type)

    