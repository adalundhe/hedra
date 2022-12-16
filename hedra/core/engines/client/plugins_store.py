from typing import Dict, Generic, Union
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.client.config import Config
from .store import ActionsStore

T = TypeVarTuple('T')


class PluginsStore(Generic[Unpack[T]]):

    def __init__(self, metadata_string: str):
        self._plugins: Dict[str, Union[Unpack[T]]] = {}
        self._config: Config = None
        self.actions = ActionsStore(metadata_string)
        self.next_name: str = None
        self.intercept: bool = False
        self.metadata_string: str = metadata_string
        self.clients = {}

    def __getitem__(self, plugin_name: str) -> Union[Unpack[T]]:

        custom_plugin: Union[Unpack[T]] = self._plugins.get(plugin_name)

        if custom_plugin.initialized is False:
            custom_plugin.name = plugin_name
            custom_plugin.actions = self.actions
            custom_plugin.initialized = True

        custom_plugin.metadata_string = self.metadata_string
        custom_plugin.next_name = self.next_name
        custom_plugin.intercept = self.intercept

        self._plugins[plugin_name] = custom_plugin
        self.clients[plugin_name] = custom_plugin

        return custom_plugin

    def __setitem__(self, plugin_name: str, plugin: Union[Unpack[T]]) -> None:
        self._plugins[plugin_name] = plugin