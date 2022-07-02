from typing import Dict
from urllib.parse import urlencode


class Params:

    def __init__(self, params: Dict[str, str]={}) -> None:
        self.data = params
        self.string = '&'.join([f'{key}={value}' for key, value in params.items()])
    
    @property
    def has_params(self):
        return len(self.data) > 0

    @property
    def encoded(self):
        return urlencode(self.string)