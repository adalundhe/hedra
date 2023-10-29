import inspect
from typing import List, Any
from .placeholder_call import PlaceholderCall


class DynamicTemplateString:

    def __init__(
        self,
        values: List[Any]
    ) -> None:
        self.values = values

        is_awaitable = len([
            inspect.isawaitable(value) for value in values
        ]) > 0 and len([
            value for value in values if isinstance(
                value, 
                PlaceholderCall
            ) and value.is_async
        ]) > 0

        self.is_async = is_awaitable

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

            elif isinstance(value, PlaceholderCall):
                value = await value.call()
                joined_string = f'{joined_string}{value}'
                
            else:
                joined_string = f'{joined_string}{value}'

        return joined_string