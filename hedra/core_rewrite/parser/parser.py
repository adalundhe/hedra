import ast
import json
import uuid
from collections import defaultdict
from typing import (
    List,
    Dict,
    Tuple,
    Any,
    Literal,
    Callable,
    Awaitable,
    Union
)
from .dynamic_placeholder import DynamicPlaceholder
from .placeholder_call import PlaceholderCall


class Parser:

    def __init__(self) -> None:
        
        self.parser_class: Any = None
        self._attributes = {}
        self.constants_count = 0
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
            ast.Await: self.parse_await
        }

        self._types = defaultdict(lambda node: node, zip(
            node_types.keys(),
            node_types.values()
        ))

        self.last_attribute: Any = None

    def parse_node(self, node: ast.AST):
        return self._types.get(
            type(node)
        )(node)

    def parse_constant(self, node: ast.Constant) -> Literal:
        self.constants_count += 1
        self._constants.append(node.value)
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

        attribute_value = self._attributes.get(node.id)

        if attribute_value:
            return attribute_value

        return DynamicPlaceholder(node.id)
    
    def parse_attribute(self, node: ast.Attribute) -> Any:

        source = self._types.get(
            type(node.value)
        )(node.value)

        attribute_name = node.attr

        attribute_value: Any = node.value
        if source == 'self':
            attribute_value = getattr(self.parser_class, attribute_name)

        self._attributes[attribute_name] = attribute_value

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
            
            if isinstance(node.value, ast.Call):

                compiled_call = compile(
                    ast.unparse(node.value),
                    '', 
                    'eval'
                )

                target_value = PlaceholderCall({
                    **target_value,
                    'call_name': compiled_call.co_names[0]
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

                target_value = PlaceholderCall({
                    **call_data,
                    'call_name': compiled_call.co_names[0],
                    'awaitable': True
                })
                
            assignments[target_node] = target_value
            self._attributes.update(assignments)

        return assignments
    
    def parse_call(self, node: ast.Call):

        self._active_trace = True

        call_id = str(uuid.uuid4())
        self._calls[call_id] = node

        self.constants_count = 0
        self._constants = []

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
        
        if 'self.client.' in call_string:
            call_string = call_string.removeprefix('self.client.')
        
            engine, method_string = call_string.split('.', maxsplit=1)
            method, _ = method_string.split('(', maxsplit=1)

            call.update({
                'source': self.parser_class.name,
                'engine': engine,
                'method': method,
                'static': len(matched_constants) == self.constants_count
            })

        self._active_trace = False
        
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