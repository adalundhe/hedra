from .plugins import (
    CustomEngine,
    CustomOptimizer,
    CustomPersona,
    CustomReporter
)

from hedra.plugins.types.common.registrar import plugin_registrar
from hedra.projects.generation.generator import Generator


class PluginGenerator(Generator):

    def __init__(self) -> None:
        super().__init__({
            'engine': CustomEngine,
            'optimizer': CustomOptimizer,
            'persona': CustomPersona,
            'reporter': CustomReporter
        }, plugin_registrar.module_paths)

    def generate_plugin(self, plugin_type: str) -> str:
        modules = self.gather_required_items(plugin_type)
        self.collect_imports(plugin_type, modules)

        self.serialize_items()

        serialized_imports = '\n'.join([
            *self.serialized_global_imports,
            *self.serialized_local_imports,

        ])

        return '\n\n'.join([
            serialized_imports,
            *self.serialized_locals
        ])