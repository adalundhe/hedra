from typing import Dict, Any
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.graphs.hooks.types.hook import Hook
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
        hook: Hook,
        hook_action: Dict[str, Any],
        persona: DefaultPersona
    ) -> None:
        self.hook = hook
        self.hook_action = hook_action
        self.persona = persona

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

    def assemble(self, action_type: RequestTypes):
        return self._assembler_types.get(
            action_type,
            assemble_plugin_action
        )(
            self.hook,
            self.hook_action,
            self.persona
        )