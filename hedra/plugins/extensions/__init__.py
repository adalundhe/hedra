from hedra.plugins.types.extension import ExtensionPlugin
from typing import Dict, Union
from .har import (
    HarConverter,
    har_extension_enabled
)



def get_enabled_extensions() -> Dict[str, Union[ExtensionPlugin, None]]:

    enabled_extensions = {
        'HarConverter': HarConverter if har_extension_enabled else None
    }
    
    return enabled_extensions