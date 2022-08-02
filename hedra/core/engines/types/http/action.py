import json
from types import FunctionType
from typing import Coroutine, Dict, Iterator, Union, List
from urllib.parse import urlencode
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.common.constants import NEW_LINE
from hedra.core.engines.types.common.fast_streams import FastWriter
from hedra.core.engines.types.common import URL
from hedra.core.engines.types.common.types import RequestTypes


class HTTPAction(BaseAction):
    
    def __init__(
        self,
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = None, 
        data: Union[str, dict, Iterator, bytes, None] = None, 
        user: str=None, 
        tags: List[Dict[str, str]] = [],  
        checks: List[FunctionType] = None, 
        hooks: Dict[str, Coroutine] = {},
    ) -> None:
        super(HTTPAction, self).__init__(
            name,
            user,
            tags,
            checks,
            hooks
        )

        self.method = method.upper()
        self.type = RequestTypes.HTTP

        address_family, protocol = self.protocols[self.type]
        self.url = URL(url, family=address_family, protocol=protocol)

        self._params = params
        self._headers = headers
        self._data = data

        self.encoded_params = None
        self.encoded_data = None
        self.encoded_headers = None
        self.is_stream = False
        self.ssl_context = None

    @property
    def size(self):
        if self.encoded_data:
            return len(self.encoded_data)

        else:
            return 0

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value
        self.encoded_data = None

    @property
    def headers(self):
        return self.headers

    @headers.setter
    def headers(self, value):
        self._headers = value
        self.encoded_headers = None

    @property
    def params(self):
        return self._params_

    @params.setter
    def params(self, value):
        self._params = value
        self.encoded_params = None
        self.encoded_headers = None

    def setup(self):
        
        if self.encoded_params is None:
            self._setup_params()

        if self.encoded_data is None:
            self._setup_data()

        if self.encoded_headers is None:
            self._setup_headers()

    def _setup_data(self):
        if self._data:
            if isinstance(self._data, Iterator):
                chunks = []
                for chunk in self._data:
                    chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                    encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                    self.size += len(encoded_chunk)
                    chunks.append(encoded_chunk)

                self.is_stream = True
                self.encoded_data = chunks

            else:

                if isinstance(self._data, dict):
                    self.encoded_data = json.dumps(
                        self._data
                    ).encode()

                elif isinstance(self._data, tuple):
                    self.encoded_data = urlencode(
                        self._data
                    ).encode()

                elif isinstance(self._data, str):
                    self.encoded_data = self._data.encode()

    def _setup_params(self):
        if self._params:
            self.encoded_params = urlencode('&'.join([
                f'{key}={value}' for key, value in self.params.items()
            ]))

    def _setup_headers(self) -> Union[bytes, Dict[str, str]]:
            path = self.url.path
            
            if self.url.query:
                path = f'{path}?{self.url.query}'

            elif self.encoded_params:
                path = f'{path}{self.encoded_params}'


            get_base = f"{self.method} {path} HTTP/1.1{NEW_LINE}"

            port = self.url.port or (443 if self.url.scheme == "https" else 80)

            hostname = self.url.parsed.hostname.encode("idna").decode()

            if port not in [80, 443]:
                hostname = f'{hostname}:{port}'

            self._headers = {
                "HOST": hostname,
                "User-Agent": "mercury-http",
                "Keep-Alive": "timeout=60, max=100000",
                "Content-Lenth": self.size,
                **self._headers
            }

            for key, value in self._headers.items():
                get_base += f"{key}: {value}{NEW_LINE}"

            self.encoded_headers = (get_base + NEW_LINE).encode()

    def write_chunks(self, writer: FastWriter):
        for chunk in self.data:
            writer.write(chunk)

        writer.write(("0" + NEW_LINE * 2).encode())
        