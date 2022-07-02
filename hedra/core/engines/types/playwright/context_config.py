class ContextConfig:

    def __init__(self, browser_type: str='chromium', device_type: str=None, locale: str=None, geolocations: str=None, permissions: str=None, color_scheme: str=None) -> None:
        self.data = {
            'browser_type': browser_type,
            'device_type': device_type,
            'locale': locale,
            'geolocations': geolocations,
            'permissions': permissions,
            'color_scheme': color_scheme
        }