import json
from http.cookies import SimpleCookie
from pydantic import (
    BaseModel,
    Json
)
from typing import (
    Dict, 
    Union, 
    List, 
    TypeVar, 
    Generic,
    Optional,
    Literal
)


T = TypeVar('T', bound=BaseModel)


class Request(Generic[T]):

    def __init__(
        self,
        path: str,
        method: Literal[
            "GET",
            "HEAD",
            "OPTIONS",
            "POST",
            "PUT",
            "PATCH",
            "DELETE",
            "TRACE"
        ],
        query: str,
        raw: List[bytes],
        model: Optional[BaseModel] = None,
    ) -> None:

        self.path = path
        self.method = method
        self._query = query

        self._headers: Dict[str, str] = {}
        self._params: Dict[str, str] = {}
        self._content: Union[bytes, None] = None
        self._data: Union[str, Json, None] = None

        self.raw = raw
        self._data_line_idx = -1
        self._model = model
        self._cookies: Union[
            Dict[str, str],
            None
        ] = None

    @property
    def headers(self):

        if self._data_line_idx == -1:
            header_lines = self.raw[1:]
            data_line_idx = 0

            for header_line in header_lines:

                if header_line == b'':
                    data_line_idx += 1
                    break
                
                key, value = header_line.decode().split(
                    ':', 
                    maxsplit=1
                )

                self._headers[key.lower()] = value.strip()

                data_line_idx += 1
            
            self._data_line_idx = data_line_idx + 1

        return self._headers
    
    @property
    def cookies(self):
        headers = self.headers

        if self._cookies is None:

            cookies = headers.get('cookie')
            self._cookies = {}

            if cookies:

                parsed_cookies = SimpleCookie()
                parsed_cookies.load(cookies)

                self._cookies =  {
                    name: morsel.value for name, morsel in parsed_cookies.items()
                }

        return self._cookies
    
    @property
    def params(self) -> Dict[str, str]:
        
        if len(self._params) < 1:
            params = self._query.split('&')

            for param in params:
                key, value = param.split('=')

                self._params[key] = value

        return self._params
    
    @property
    def content(self):
        if self._content is None:
            self._content = b''.join(self.raw[self._data_line_idx:]).strip()

        return self._content
    
    @content.setter
    def content(self, updated: bytes):
        self._content = updated

    @property
    def body(self):
        
        headers = self.headers

        if self._data is None:
            self._data = self.content

            if headers.get('content-type') == 'application/json':
                self._data = json.loads(self._data)

        return self._data
    
    def data(self) -> Union[bytes, str, Dict[str, str], T]:
        data = self.body

        if isinstance(data, dict) and self._model:
            return self._model(**data)
        
        return data