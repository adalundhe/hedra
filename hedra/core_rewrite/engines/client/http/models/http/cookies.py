from typing import List, Union, Dict
from .types import (
    HTTPCookie, 
    HTTPCookieMap,
    HTTPEncodableValue
)

def map_cookie_value(cookie_value: str) -> HTTPEncodableValue:
    match cookie_value.lower():
                        
        case int():
            return int(cookie_value)
        
        case float():
            return float(cookie_value)
        
        case 'true':
            return True
        
        case 'false':
            return False
        
        case 'none':
            return None
        
        case _:
            return cookie_value


class Cookies:

    def __init__(self) -> None:
        self._data = b''
        self._split: Union[
            List[HTTPCookie],
            None
        ]=None

        self._parsed: HTTPCookieMap=None

    def __getitem__(self, name: str):
        return self.parsed.get(name)
            
    def __iter__(self):
        for cookie_name, cookie_value in self.parsed.items():
            yield cookie_name, cookie_value

    def update(self, data: str):
        self._data += data
            
    @property
    def parsed(self) -> HTTPCookieMap:
        
        if self._split is None:
            self._split = [
                cookie.split(
                    b'='
                ) for cookie in self._data.split(b';')
            ]

        if self._parsed is None:
            self._parsed: HTTPCookieMap = {}
            
            for cookie in self._split:
                if len(cookie) == 1:
                    cookie_name = cookie[0]
                    self._parsed[cookie_name] = cookie_name

                elif len(cookie) == 2:
                    cookie_name, cookie_value = cookie
                    self._parsed[cookie_name] = map_cookie_value(
                        cookie_value
                    )

                else:
                    cookie_name = cookie[0]
                    self._parsed[cookie_name] = map_cookie_value(
                        b'='.join(cookie[1:])
                    )


        return self._parsed
