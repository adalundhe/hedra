from __future__ import annotations
from typing import (
    Dict,
    Iterator,
    List,
    Union
)
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common.url import URL
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.action import HTTPAction



class HTTP3Action(HTTPAction):

    def __init__(
            self, 
            name: str, 
            url: str, 
            method: str = 'GET', 
            headers: Dict[str, str]={}, 
            data: Union[str, dict, Iterator, bytes, None] = None, 
            user: str = None, tags: 
            List[Dict[str, str]] = ..., redirects: int = 3
        ) -> None:

        super().__init__(
            name, 
            url, 
            method, 
            headers, 
            data, 
            user, 
            tags, 
            redirects
        )


        self.type = RequestTypes.HTTP3
        address_family, protocol = self.protocols[self.type]
        self.url = URL(url, family=address_family, protocol=protocol)


        self.redirects = redirects
        self.hooks: Hooks[HTTP3Action] = Hooks()
    
    def _setup_headers(self) -> Union[bytes, Dict[str, str]]:

        self.encoded_headers = [
            (b":method", self.method.encode()),
            (b":scheme", self.url.scheme.encode()),
            (b":authority", self.url.authority.encode()),
            (b":path", self.url.full.encode()),
            (b"user-agent", 'hedra/client'.encode()),
        ]
        
        self.encoded_headers.extend([
            (
                k.encode(), 
                v.encode()
            ) for (
                k, 
                v
            ) in self.headers.items()
        ])

    