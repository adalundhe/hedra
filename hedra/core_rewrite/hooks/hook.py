import inspect
import pickle
import ast
import textwrap
from collections import defaultdict
from inspect import signature
from typing import (
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Type,
    Union,
    Any,
    get_args
)
from hedra.core_rewrite.parser import Parser
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.common.base_result import BaseResult



class Hook:

    def __init__(
        self,
        call: Callable[
            ...,
            Awaitable[Any] | Awaitable[BaseAction] | Awaitable[BaseResult]
        ],
        dependencies: List[str]
    ) -> None:

        call_signature = signature(call)

        self.call = call
        self.full_name = call.__qualname__ 
        self.name = call.__name__
        self.workflow = self.full_name.split('.').pop(0)
        self.dependencies = dependencies

        self.params = call_signature.parameters
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
            } for arg in self.params.values() if arg.KEYWORD_ONLY
        }

        self.static = len(self.params) <= 1

        self.return_type = call_signature.return_annotation
        self.is_test = False

        annotation_subtypes = list(get_args(self.return_type))

        if len(annotation_subtypes) > 0:
            self.return_type = [
                return_type for return_type in annotation_subtypes
            ]

        else:
            self.is_test = issubclass(
                self.return_type,
                BaseResult
            )

        self.cache: Dict[str, Any] = {}
        self.parser = Parser()
        self._tree = ast.parse(
            textwrap.dedent(
                inspect.getsource(call)
            )
        )

    def setup(self):
        
        for node in ast.walk(self._tree):

            if isinstance(node, ast.Assign):
                result = self.parser.parse_assign(node)

            if isinstance(node, ast.Attribute):
                result = self.parser.parse_attribute(node)
        
        for node in ast.walk(self._tree):
            if isinstance(node, ast.Call):

                result = self.parser.parse_call(node)
                engine = result.get('engine')

                if engine:
                    
                    parser_class = self.parser.parser_class_name
                    method = result.get('method')
                    self.static = result.get('static')

                    source_fullname = f'{parser_class}.client.{engine}.{method}'
                    result['source'] = source_fullname


                    self.cache[source_fullname] = result



        





