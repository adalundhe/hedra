import orjson
from pydantic import (
    BaseModel,
    StrictStr,
    StrictBytes,
    StrictInt
)
from typing import (
    Dict, 
    List,
    Optional, 
    Tuple, 
    Union,
    Literal
)
from urllib.parse import urlencode
from .url import URL
from .types import HTTPCookie, HTTPEncodableValue


NEW_LINE = '\r\n'


class HTTPRequest(BaseModel):
    url: StrictStr
    method: Literal[
        "GET", 
        "POST",
        "HEAD",
        "OPTIONS", 
        "PUT", 
        "PATCH", 
        "DELETE"
    ]
    cookies: Optional[List[HTTPCookie]]=None
    auth: Optional[Tuple[str, str]]=None
    params: Optional[Dict[str, HTTPEncodableValue]]=None
    headers: Dict[str, str]={}
    data: Union[
        Optional[StrictStr],
        Optional[StrictBytes],
        Optional[BaseModel]
    ]=None
    redirects: StrictInt=3

    class Config:
        arbitrary_types_allowed=True

    def prepare(
        self,
        url: URL
    ):
        
        url_path = url.path

        if self.params and len(self.params) > 0:
            url_params = urlencode(self.params)
            url_path += f'?{url_params}'

        get_base = f"{self.method} {url.path} HTTP/1.1{NEW_LINE}"

        port = url.port or (443 if url.scheme == "https" else 80)

        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f'{hostname}:{port}'

        if isinstance(self.data, BaseModel):
            data = orjson.dumps({
                name: value for name, value in self.data.__dict__.items() if value is not None
            })
            self.headers['content-type'] = 'application/json'

            size = len(data)

        elif isinstance(self.data, str):
            data = self.data.encode()
            size = len(data)

        elif self.data:
            data = self.data
            size = len(self.data)

        else:
            data = self.data
            size = 0

        header_items = [
            ("HOST", hostname),
            ("User-Agent", "mercury-http"),
            ("Keep-Alive", "timeout=60, max=100000"),
            ("Content-Length", size)
        ]

        header_items.extend(self.headers.items())

        for key, value in header_items:
            get_base += f"{key}: {value}{NEW_LINE}"

        if self.cookies:
    
            cookies = []

            for cookie_data in self.cookies:
                if len(cookie_data) == 1:
                    cookies.append(cookie_data[0])

                elif len(cookie_data) == 2:
                    cookie_name, cookie_value = cookie_data
                    cookies.append(f'{cookie_name}={cookie_value}')

            cookies = '; '.join(cookies)
            get_base += f'Set-Cookie: {cookies}{NEW_LINE}'

        return (get_base + NEW_LINE).encode(), data
