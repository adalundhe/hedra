import inspect

from .plugin_types import (
    CustomReporter,
    CustomReporterConfig,
    CustomAction,
    CustomResult,
    CustomEngine,
    CustomOptimizer,
    CustomPersona
)

from hedra.plugins.types.reporter import (
    ReporterConfig,
    ReporterPlugin
)

from hedra.plugins.types.engine import (
    EnginePlugin,
    Action,
    Result
)

from hedra.plugins.types.optimizer import (
    OptimizerPlugin
)

from hedra.plugins.types.persona import (
    PersonaPlugin
)



class PluginGenerator:

    def __init__(self) -> None:
        pass

    def generate_reporter(self):
        imports = [
            'from typing import List',
            'from hedra.plugins.types.common import Event'
        ]
        
        reporter_hooks = [
            'connect',
            'close',
            'process_events',
            'process_shared',
            'process_metrics',
            'process_custom',
            'process_errors'
        ]

        hook_imports = [
            f'{reporter_hook},' for reporter_hook in reporter_hooks
        ]

        reporter_plugin_imports = [
            ReporterConfig.__name__,
            ReporterPlugin.__name__,
            'Metrics',
        ]

        plugin_imports = [
            f'{reporter_plugin_import},' for reporter_plugin_import in reporter_plugin_imports
        ]

        reporter_classes = [
            CustomReporterConfig,
            CustomReporter
        ]

        reporter_imports = []
        reporter_imports.extend(plugin_imports)
        reporter_imports.extend(hook_imports)

        serialized_reporter_imports = '\n\t'.join(reporter_imports)

        imports.append(
            f'from hedra.plugins.types.reporter import (\n\t{serialized_reporter_imports}\n)'
        )

        serialized_imports = '\n'.join(imports)

        serialized_classes = [
            inspect.getsource(reporter_class) for reporter_class in reporter_classes
        ]
        
        return '\n\n'.join([
            f'{serialized_imports}\n',
            *serialized_classes
        ])

    def generate_engine(self):

        imports = [
            'from typing import Any, Dict, List'
        ]

        engine_hooks = [
            'connect',
            'execute',
            'close'
        ]

        hook_imports = [
            f'{engine_hook},' for engine_hook in engine_hooks
        ]

        engine_plugin_imports = [
            EnginePlugin.__name__,
            Action.__name__,
            Result.__name__
        ]

        plugin_imports = [
            f'{engine_plugin_import},' for engine_plugin_import in engine_plugin_imports
        ]

        engine_classes = [
            CustomAction,
            CustomResult,
            CustomEngine
        ]

        engine_imports = []
        engine_imports.extend(plugin_imports)
        engine_imports.extend(hook_imports)

        serialized_engine_imports = '\n\t'.join(engine_imports)

        imports.append(
            f'from hedra.plugins.types.engine import (\n\t{serialized_engine_imports}\n)'
        )

        serialized_imports = '\n'.join(imports)

        serialized_classes = [
            inspect.getsource(engine_class) for engine_class in engine_classes
        ]
        
        return '\n\n'.join([
            f'{serialized_imports}\n',
            *serialized_classes
        ])

    def generate_optimizer(self):
        imports = [
            'from types import FunctionType',
            'from typing import Any, Dict'
        ]

        optimize_hooks = [
            'get',
            'optimize',
            'update'
        ]

        hook_imports = [
            f'{optimize_hook},' for optimize_hook in optimize_hooks
        ]

        optimizer_plugin_imports = [OptimizerPlugin.__name__]

        plugin_imports = [
            f'{optimizer_plugin_import},' for optimizer_plugin_import in optimizer_plugin_imports
        ]

        optimize_classes = [CustomOptimizer]

        optimize_imports = []
        optimize_imports.extend(plugin_imports)
        optimize_imports.extend(hook_imports)

        serialized_optimize_imports = '\n\t'.join(optimize_imports)

        imports.append(
            f'from hedra.plugins.types.optimizer import (\n\t{serialized_optimize_imports}\n)'
        )

        serialized_imports = '\n'.join(imports)

        serialized_classes = [
            inspect.getsource(optimize_class) for optimize_class in optimize_classes
        ]
        
        return '\n\n'.join([
            f'{serialized_imports}\n',
            *serialized_classes
        ])

    def generate_persona(self):
        imports = [
            'import time',
            'import asyncio',
            'from typing import AsyncIterable, Dict, List',
            'from hedra.core.graphs.hooks.types.hook_types import HookType',
            'from hedra.core.graphs.hooks.types.hook import Hook'
        ]

        persona_hooks = [
            'setup',
            'generate',
            'shutdown'
        ]

        hook_imports = [
            f'{persona_hook},' for persona_hook in persona_hooks
        ]

        persona_plugin_imports = [PersonaPlugin.__name__]

        plugin_imports = [
            f'{persona_plugin_import},' for persona_plugin_import in persona_plugin_imports
        ]

        persona_classes = [CustomPersona]

        persona_imports = []
        persona_imports.extend(plugin_imports)
        persona_imports.extend(hook_imports)

        serialized_persona_imports = '\n\t'.join(persona_imports)

        imports.append(
            f'from hedra.plugins.types.persona import (\n\t{serialized_persona_imports}\n)'
        )

        serialized_imports = '\n'.join(imports)

        serialized_classes = [
            inspect.getsource(persona_class) for persona_class in persona_classes
        ]
        
        return '\n\n'.join([
            f'{serialized_imports}\n',
            *serialized_classes
        ])
