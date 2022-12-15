from typing import Dict, List, Any

class ContextConfig:

    __slots__ = (
        'data',
        'options'
    )

    def __init__(
        self, 
        browser_type: str='chromium', 
        device_type: str=None, 
        locale: str=None, 
        geolocation: Dict[str, float]=None, 
        permissions: List[str]=[], 
        color_scheme: str=None,
        options: Dict[str, Any]={}
    ) -> None:
        self.data = {
            'browser_type': browser_type,
            'device_type': device_type,
            'locale': locale,
            'geolocation': geolocation,
            'permissions': permissions,
            'color_scheme': color_scheme
        }

        self.options = options