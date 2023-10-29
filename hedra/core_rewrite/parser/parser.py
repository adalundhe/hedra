import ast
import json
import inspect
import uuid
import asyncio
from collections import defaultdict
from typing import (
    List,
    Dict,
    Tuple,
    Any,
    Literal,
    Callable,
    Union,
    get_origin
)
from types import FunctionType
from .dynamic_placeholder import DynamicPlaceholder
from .dynamic_template_string import DynamicTemplateString
from .placeholder_call import PlaceholderCall


class Parser:

    def __init__(self) -> None:
        
        self.parser_class: Any = None
        self.parser_class_name: Union[str, None] = None
        self.attributes = {}
        self._constants = []
        self._calls: Dict[str, Callable[..., Any]] = {}
        self._active_trace: bool = False

        node_types = {
            ast.Constant: self.parse_constant,
            ast.Dict: self.parse_dict,
            ast.List: self.parse_list,
            ast.Tuple: self.parse_tuple,
            ast.Name: self.parse_name,
            ast.Attribute: self.parse_attribute,
            ast.Assign: self.parse_assign,
            ast.Call: self.parse_call,
            ast.keyword: self.parse_keyword,
            ast.Await: self.parse_await,
            ast.JoinedStr: self.parse_joined_string,
            ast.FormattedValue: self.parse_formatted_value
        }

        self._types = defaultdict(lambda node: node, zip(
            node_types.keys(),
            node_types.values()
        ))

    def parse_node(self, node: ast.AST):
        return self._types.get(
            type(node)
        )(node)

    def parse_constant(self, node: ast.Constant) -> Literal:
        return node.value
    
    def parse_list(self, node: ast.List) -> List[Any]:

        return [
            self._types.get(
                type(node_val)
            )(node_val) for node_val in node.elts
        ]
    
    def parse_dict(self, node: ast.Dict) -> Dict[Any, Any]:

        keys = [
            self._types.get(
                type(key_val)
            )(key_val) for key_val in node.keys
        ]

        values = [
            self._types.get(
                type(value_val)
            )(value_val) for value_val in node.values
        ]

        return dict(zip(keys, values))

    def parse_tuple(self, node: ast.Tuple) -> Tuple[Any, ...]:
        return [
            self._types.get(
                type(node_val)
            )(node_val) for node_val in node.elts if node_val is not None
        ]
    
    def parse_name(self, node: ast.Name) -> Any:

        attribute_value = self.attributes.get(node.id)

        if attribute_value:
            return attribute_value

        return DynamicPlaceholder(node.id)
    
    def parse_attribute(self, node: ast.Attribute) -> Any:


        if isinstance(node.value, ast.Name):
            source = node.value.id

        else:
            source = self._types.get(
                type(node.value)
            )(node.value)

        attribute_name = node.attr
        attribute_value: Any = node.value
        
        source_instance = self.attributes.get(
            source,
            self.parser_class
        )

        if hasattr(source_instance, attribute_name):
            attribute_value = getattr(source_instance, attribute_name)

        self.attributes[attribute_name] = attribute_value

        return attribute_value
    
    def parse_assign(self, node: ast.Assign) -> Any:

        assignments: Dict[str, Any] = {}
        
        for target in node.targets:

            target_node = self._types.get(
                type(target)
            )(target)

            target_value = self._types.get(
                type(node.value)
            )(node.value)

            if isinstance(target_value, ast.Name) and isinstance(node.value, ast.Attribute):
                instance = self.attributes.get(
                    target_value.id,
                    self.parser_class
                )

                if hasattr(instance, node.value.attr):
                    target_value = getattr(instance, node.value.attr)
                    self.attributes[node.value.attr] = target_value

            if isinstance(node.value, ast.Call):

                call_data: Dict[str, Any] = target_value
                is_static = call_data.get('static')


                compiled_call = compile(
                    ast.unparse(node.value),
                    '', 
                    'eval'
                )

                call_name = compiled_call.co_names[0]
                call_item = self.attributes.get(call_name)

                call_args: List[Any] = call_data.get('args')
                call_kwargs: Dict[str, Any] = call_data.get('kwargs')
                call_is_static = call_data.get('static')

                return_is_static = get_origin(
                    inspect.signature(call_item).return_annotation
                ) == Literal

                no_arguments = len(
                    inspect.signature(call_item).parameters
                ) == 0
                
                is_static = call_is_static and return_is_static and no_arguments

                is_async = inspect.isawaitable(call_item)
                
                if is_static and call_item and is_async is False:
                    target_node = target.id if isinstance(target, ast.Name) else target_node

                    args = [
                        arg.get('value') for arg in call_args
                    ]

                    kwargs = {
                        name: arg.get('value') for name, arg, in call_kwargs.items()
                    }

                    target_value = call_item(
                        *args,
                        **kwargs
                    )
                    
                else:

                    call_name = compiled_call.co_names[0]

                    target_value = PlaceholderCall({
                        **target_value,
                        'call': self.attributes.get(call_name),
                        'call_name': call_name,
                        'awaitable': is_async
                    })

            elif isinstance(node.value, ast.Await):

                compiled_call = compile(
                    ast.unparse(target_value),
                    '', 
                    'eval'
                )

                call_data = self._types.get(
                    type(target_value)
                )(target_value)

                call_name = compiled_call.co_names[0]

                if inspect.iscoroutine(call_data):

                    call_name = call_data.__qualname__
                    call = self.attributes.get(call_name)

                    call_signature = inspect.signature(call)
                    call_args = call_signature.parameters.values()

                    call_data = {
                        'args': [
                            DynamicPlaceholder(
                                arg.name
                            ) for arg in call_args if arg.default is None
                        ],
                        'kwargs': {
                            arg.name: DynamicPlaceholder(
                                arg.name
                            ) for arg in call_args if arg.default
                        }
                    }  

                target_value = PlaceholderCall({
                    **call_data,
                    'call': self.attributes.get(call_name),
                    'call_name': call_name,
                    'awaitable': True
                })

            elif isinstance(target, ast.Name) and isinstance(target_value, ast.expr) is False:
                target_node = target.id
                
            assignments[target_node] = target_value
            self.attributes.update(assignments)

        return assignments
    
    def parse_call(self, node: ast.Call):

        call_id = str(uuid.uuid4())
        self._calls[call_id] = node

        call = {
            'call_id': call_id,
            'source': self._types.get(
                type(node.func)
            )(node.func),
            'args': [
                {
                    'value': self._types.get(
                        type(arg)
                    )(arg)
                } for arg in node.args if arg
            ],
            'kwargs': [
                self._types.get(
                    type(arg)
                )(arg) for arg in node.keywords if arg
            ]
        }

        matched_constants = []
        all_args: List[
            Dict[str, Any]
        ] = [
            *call['args'],
            *call['kwargs']
        ]

        for arg in all_args:

            try:

                arg_value = arg.get('value')
                json.dumps(arg_value)

                arg['type'] = 'static'
                matched_constants.append(arg)

            except Exception:
                arg['type'] = 'dynamic'

        call_kwargs = {}
        for arg in call['kwargs']:
            kwarg_name = arg['name']
            call_kwargs[kwarg_name] = {
                'type': arg['type'],
                'value': arg['value']
            }

        call['kwargs'] = call_kwargs

        call_string = ast.unparse(node)

        all_args_count = len(call['args']) + len(call['kwargs'])
        call['static'] = len(matched_constants) == all_args_count

        if 'self.client.' in call_string:
            call_string = call_string.removeprefix('self.client.')
        
            engine, method_string = call_string.split('.', maxsplit=1)
            method, _ = method_string.split('(', maxsplit=1)

            call.update({
                'source': self.parser_class_name,
                'engine': engine,
                'method': method
            })
        
        return call
    
    def parse_keyword(self, node: ast.keyword) -> Any:
        return {
            'name': node.arg,
            'value': self._types.get(
                type(node.value)
            )(node.value)
        }
    
    def parse_await(self, node: ast.Await) -> Any:
        return node.value
    
    def parse_joined_string(self, node: ast.JoinedStr) -> Any:

        values = [
            self._types.get(
                type(arg)
            )(arg) for arg in node.values
        ]

        joined_values = ''

        for value in values:
            try:
                json.dumps(value)
                joined_values = f'{joined_values}{value}'

            except Exception:
                joined_values = DynamicTemplateString(values)
                break
                    

        return joined_values

    def parse_formatted_value(self, node: ast.FormattedValue) -> Any:
        result = self._types.get(
            type(node.value)
        )(node.value)

        return result

        