import inspect
from typing import List, Any


class DynamicTemplateString:

    def __init__(
        self,
        values: List[Any]
    ) -> None:
        self.values = values
        self.is_async = len([
            inspect.isawaitable(value) for value in values
        ])

    async def eval_string_async(self, values: List[Any]) -> str:
        
        joined_string = ''

        for value in values:
            if inspect.iscoroutinefunction(value):
                value = await value()
                joined_string = f'{joined_string}{value}'

            elif inspect.isawaitable(value):
                value = await value
                joined_string = f'{joined_string}{value}'

            elif inspect.isfunction(value):
                value = value()
                joined_string = f'{joined_string}{value}'

            else:
                joined_string = f'{joined_string}{value}'

        return joined_string