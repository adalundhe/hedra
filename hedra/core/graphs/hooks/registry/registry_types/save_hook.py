from typing import Type, Callable, Awaitable, Any
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.tools.filesystem import open
from .hook import Hook


class SaveHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        key: str=None,
        checkpoint_filepath: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.SAVE
        )

        self.context_key = key
        self.save_path = checkpoint_filepath


    async def call(self, context: SimpleContext) -> None:

        save_file = await open(self.save_path, 'w')
        await save_file.write(
            await self._call(
                context.get(self.context_key)
            )
        )

        await save_file.close()


        if context.get(self.context_key):
            context[self.context_key] = None


        

