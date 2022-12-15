from typing import Dict, Any
from hedra.core.engines import registered_engines
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.types.playwright import PlaywrightCommand
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.types.common.types import RequestTypes


def assemble_playwright_command(
    hook: Hook,
    hook_action: Dict[str, Any],
    persona: DefaultPersona
):
    pass