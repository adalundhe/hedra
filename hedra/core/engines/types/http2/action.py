import json
import h2.settings
from typing import Dict, Iterator, Union, List
from urllib.parse import urlencode
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.common.constants import NEW_LINE
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common import URL
from hedra.core.engines.types.common.encoder import Encoder
from hedra.core.engines.types.common.types import RequestTypes


class HTTP2Action(BaseAction):

    __slots__ = (
        'action_id',
        'method',
        'type',
        'url',
        'protocols',
        '_headers',
        '_data',
        'encoded_data',
        'encoded_headers',
        'is_stream',
        'ssl_context',
        'hpack_encoder',
        '_remote_settings'
    )
    
    def __init__(
        self,
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        data: Union[str, dict, Iterator, bytes, None] = None, 
        user: str=None, 
        tags: List[Dict[str, str]] = []
    ) -> None:
        super(HTTP2Action, self).__init__(
            name,
            user,
            tags
        )

        self.method = method.upper()
        self.type = RequestTypes.HTTP2

        address_family, protocol = self.protocols[self.type]
        self.url = URL(url, family=address_family, protocol=protocol)

        self._headers = headers
        self._data = data

        self.encoded_data = None
        self.encoded_headers = None
        self.is_stream = False
        self.ssl_context = None
        self.hpack_encoder = Encoder()
        self._remote_settings = h2.settings.Settings(
            client=False
        )
        
        self.hooks: Hooks[HTTP2Action] = Hooks()

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
        return self._headers

    @headers.setter
    def headers(self, value):
        self._headers = value
        self.encoded_headers = None

    def setup(self):

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

    def _setup_headers(self) -> Union[bytes, Dict[str, str]]:
    
        encoded_headers = [
            (b":method", self.method),
            (b":authority", self.url.authority),
            (b":scheme", self.url.scheme),
            (b":path", self.url.path),
        ]

        encoded_headers.extend([
            (
                k.lower(), 
                v
            )
            for k, v in self._headers.items()
            if k.lower()
            not in (
                b"host",
                b"transfer-encoding",
            )
        ])
        
        encoded_headers = self.hpack_encoder.encode(encoded_headers)
        self.encoded_headers = [
            encoded_headers[i:i+self._remote_settings.max_frame_size]
            for i in range(
                0, len(encoded_headers), self._remote_settings.max_frame_size
            )
        ]

    def to_serializable(self):

        return {
            'name': self.name,
            'type': self.type,
            'method': self.method,
            'url': {
                'ip_addr': self.url.ip_addr,
                'port': self.url.port,
                'url': self.url.full,
                'socket_config': self.url.socket_config,
                'is_ssl': self.url.is_ssl
            },
            'headers': {
                'headers': self._headers,
                'encoded_headers': self.encoded_headers
            },
            'data': {
                'data': self._data,
                'encoded_data': self.encoded_data
            },

            'metadata': {
                'user': self.metadata.user,
                'tags': self.metadata.tags
            },
            'hooks': self.hooks.to_serializable()
        }
        
        