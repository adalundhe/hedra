from typing import List


class InvalidPluginType(Exception):

    def __init__(self, plugin_type: str, valid_types: List[str]) -> None:

        valid_plugin_types = '\n-'.join(valid_types)

        super().__init__(
            f'\n\nError - invalid plugin type - {plugin_type} - specified.\n\nValid types are: \n-{valid_plugin_types}\n'
        )