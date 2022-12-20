from typing import Dict, Any, Awaitable, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.personas.types.default_persona import DefaultPersona
from .assemble_graphql_action import assemble_graphql_action
from .assemble_graphql_http2_action import assemble_graphql_http2_action
from .assemble_grpc_action import assemble_grpc_action
from .assemble_http_action import assemble_http_action
from .assemble_http2_action import assemble_http2_action
from .assemble_playwright_action import assemble_playwright_command
from .assemble_udp_action import assemble_udp_action
from .assemble_plugin_action import assemble_plugin_action
from .assemble_task import assemble_task
from .assemble_websocket_action import assemble_websocket_action


class ActionAssembler:

    def __init__(
        self,
        hook: Union[ActionHook, TaskHook],
        hook_action: Dict[str, Any],
        persona: DefaultPersona,
        config: Config,
        metadata_string: str
    ) -> None:
        self.hook = hook
        self.hook_action = hook_action
        self.persona = persona
        self.config = config

        self._assembler_types = {
            RequestTypes.GRAPHQL: assemble_graphql_action,
            RequestTypes.GRAPHQL_HTTP2: assemble_graphql_http2_action,
            RequestTypes.GRPC: assemble_grpc_action,
            RequestTypes.HTTP: assemble_http_action,
            RequestTypes.HTTP2: assemble_http2_action,
            RequestTypes.PLAYWRIGHT: assemble_playwright_command,
            RequestTypes.TASK: assemble_task,
            RequestTypes.UDP: assemble_udp_action,
            RequestTypes.WEBSOCKET: assemble_websocket_action
        }

        self.metadata_string = metadata_string

    def assemble(self, action_type: RequestTypes) -> Awaitable[Union[ActionHook, TaskHook]]:
        return self._assembler_types.get(
            action_type,
            assemble_plugin_action
        )(
            self.hook,
            self.hook_action,
            self.persona,
            self.config,
            self.metadata_string
        )