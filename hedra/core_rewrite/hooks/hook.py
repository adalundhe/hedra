import ast
import inspect
import textwrap
from collections import defaultdict
from inspect import signature
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Type,
    Union,
    get_args,
)

from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts
from hedra.core_rewrite.parser import Parser

from .call_arg import CallArg


class Hook:
    def __init__(
        self,
        call: Callable[
            ..., Awaitable[Any] | Awaitable[BaseAction] | Awaitable[BaseResult]
        ],
        dependencies: List[str],
        timeouts: Optional[Timeouts] = None,
    ) -> None:
        if timeouts is None:
            timeouts = Timeouts()

        call_signature = signature(call)

        self.call = call
        self.full_name = call.__qualname__
        self.name = call.__name__
        self.workflow = self.full_name.split(".").pop(0)
        self.dependencies = dependencies
        self._timeouts = timeouts
        self.call_ids: List[str] = []

        self.params = call_signature.parameters
        self.args: Dict[
            str,
            Dict[
                Union[Literal["annotation"], Literal["default"]], Union[Type[Any], Any]
            ],
        ] = {
            arg.name: {"annotation": arg.annotation, "default": arg.default}
            for arg in self.params.values()
            if arg.KEYWORD_ONLY
        }

        self.static = len([param for param in self.params if param != "self"]) == 0

        self.return_type = call_signature.return_annotation
        self.is_test = False

        annotation_subtypes = list(get_args(self.return_type))

        if len(annotation_subtypes) > 0:
            self.return_type = [return_type for return_type in annotation_subtypes]

        else:
            self.is_test = issubclass(self.return_type, BaseResult)

        self.cache: Dict[
            str, Dict[str, Dict[str, Union[List[Dict[str, Any]], Dict[str, Any]]]]
        ] = defaultdict(dict)
        self.parser = Parser()
        self._tree = ast.parse(textwrap.dedent(inspect.getsource(call)))

    def __iter__(self):
        for call_id in self.call_ids:
            yield call_id

    def setup(self, context: Dict[str, Any]):
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
                engine = result.get("engine")
                call_source = result.get("source")
                call_id = result.get("call_id")
                method = result.get("method")

                if engine:
                    self.call_ids.append(call_id)

                    parser_class = self.parser.parser_class_name
                    self.static = result.get("static")

                    source_fullname = f"{parser_class}.client.{engine}.{method}"

                elif isinstance(call_source, ast.Attribute):
                    source_fullname = call_source.attr

                elif inspect.isfunction(call_source) or inspect.ismethod(call_source):
                    source_fullname = call_source.__qualname__

                else:
                    source_fullname = call_source

                is_cacheable_call = source_fullname != "step" and (
                    engine is not None or self.is_test is False
                )

                if is_cacheable_call:
                    result["source"] = source_fullname
                    self.cache[source_fullname][call_id] = result

    @property
    def args_map(self):
        call_args: Dict[str, List[CallArg]] = defaultdict(list)
        for call_name, calls in self.cache.items():
            for call_id, call_data in calls.items():
                call_id = call_data.get("call_id")
                args = call_data.get("args")
                timeouts = call_data.get("kwargs", {}).get("timeouts", self._timeouts)

                call_args[call_id].extend(
                    [
                        CallArg(
                            call_name=call_name,
                            call_id=call_id,
                            arg_type="arg",
                            position=arg_postition,
                            workflow=self.workflow,
                            engine=call_data.get("engine"),
                            method=call_data.get("method"),
                            value=arg.get("value"),
                            data_type=arg.get("type", "static"),
                            timeouts=timeouts,
                        )
                        for arg_postition, arg in enumerate(args)
                    ]
                )

        return call_args

    @property
    def kwargs_map(self):
        call_args: Dict[str, List[CallArg]] = defaultdict(list)
        for call_name, calls in self.cache.items():
            for call_id, call_data in calls.items():
                call_id = call_data.get("call_id")
                kwargs = call_data.get("kwargs", {})

                call_timeouts = kwargs.get("timeouts", self._timeouts)

                for arg_name, arg in kwargs.items():
                    call_args[call_id].extend(
                        [
                            CallArg(
                                call_name=call_name,
                                call_id=call_id,
                                arg_name=arg_name,
                                arg_type="kwarg",
                                workflow=self.workflow,
                                engine=call_data.get("engine"),
                                method=call_data.get("method"),
                                value=arg.get("value"),
                                data_type=arg.get("type", "static"),
                                timeouts=call_timeouts,
                            )
                        ]
                    )

        return call_args

    @property
    def static_args(self):
        static_args: Dict[str, List[str]] = defaultdict(list)

        call_args = self.args_map
        for call_id in self.args_map:
            call_args[call_id].extend(self.kwargs_map[call_id])

        for call_id, args in call_args.items():
            static_call_args = [arg for arg in args if arg.data_type == "static"]
            if len(static_call_args) > 0:
                static_args[call_id].extend(static_call_args)

        return static_args
