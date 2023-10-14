import inspect
from inspect import signature
from typing import (
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Type,
    Union,
    Any
)


class Hook:

    def __init__(
        self,
        call: Callable[
            ...,
            Awaitable[Any]
        ],
        dependencies: List[str]
    ) -> None:

        call_signature = signature(call) 

        self.call = call
        self.full_name = call.__qualname__ 
        self.name = call.__name__
        self.workflow = self.full_name.split('.').pop(0)
        self.dependencies = dependencies
        self.args: Dict[
            str,
            Dict[
                Union[
                    Literal['annotation'],
                    Literal['default']
                ],
                Union[
                    Type[Any],
                    Any
                ]
            ]
        ] = {
            arg.name: {
                'annotation': arg.annotation,
                'default': arg.default
            } for arg in call_signature.parameters.values() if arg.KEYWORD_ONLY
        }


        self.static = len(self.args) == 0
        self.is_test = isinstance(call_signature.return_annotation, )

        





