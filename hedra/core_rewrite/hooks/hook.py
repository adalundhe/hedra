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

        self.static = len([
            param for param in self.params if param != 'self'
        ]) == 0

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

        self.cache: Dict[str, List[Any]] = defaultdict(dict)
        self.parser = Parser()
        self._tree = ast.parse(
            textwrap.dedent(
                inspect.getsource(call)
            )
        )

    def setup(
        self,
        context: Dict[str, Any]
    ):
        
        self.parser.attributes.update(context)

        for cls in inspect.getmro(self.call.__self__.__class__):
            if self.call.__name__ in cls.__dict__: 
                
                self.parser.parser_class = cls
                self.parser.parser_class_name = self.workflow

                break
        
        for node in ast.walk(self._tree):

            if isinstance(node, ast.Assign):
                result = self.parser.parse_assign(node)

            if isinstance(node, ast.Attribute):
                result = self.parser.parse_attribute(node)
        
        for node in ast.walk(self._tree):
            if isinstance(node, ast.Call):

                result = self.parser.parse_call(node)
                engine = result.get('engine')
                call_source = result.get('source')

                if engine:
                    
                    parser_class = self.parser.parser_class_name
                    method = result.get('method')
                    self.static = result.get('static')

                    source_fullname = f'{parser_class}.client.{engine}.{method}'

                elif isinstance(call_source, ast.Attribute):
                    source_fullname = call_source.attr

                elif inspect.isfunction(call_source) or inspect.ismethod(call_source):
                    source_fullname = call_source.__qualname__

                else:
                    source_fullname = call_source

                is_cacheable_call = (
                    source_fullname != 'step' and (
                        engine is not None or self.is_test is False
                    )
                )

                if is_cacheable_call:
                    call_id = result.get('call_id')
                    result['source'] = source_fullname
                    self.cache[source_fullname][call_id] = result 
