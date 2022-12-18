from typing import Dict, Iterator, Union, List
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.action import HTTPAction
from .utils import (
    pack_hostname,
    create_sec_websocket_key
)
from .constants import WEBSOCKETS_VERSION


class WebsocketAction(HTTPAction):

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

        super(
            WebsocketAction,
            self
        ).__init__(
            name, 
            url, 
            method, 
            headers, 
            data, 
            user, 
            tags
        )

        self.type = RequestTypes.WEBSOCKET
        self.hooks: Hooks[WebsocketAction] = Hooks()

    def _setup_headers(self):

        for header_name, header_value in self._headers.items():
            header_name_lowered = header_name.lower()
            self._headers[header_name_lowered] = header_value

        headers = [
            "GET %s HTTP/1.1" % self.url.path,
            "Upgrade: websocket"
        ]
        if self.url.port == 80 or self.url.port == 443:
            hostport = pack_hostname(self.url.hostname)
        else:
            hostport = "%s:%d" % (pack_hostname(
                self.url.hostname
            ), self.url.port)

        host = self._headers.get("host")
        if host:
            headers.append(f"Host: {host}")
        else:
            headers.append(f"Host: {hostport}")

        # scheme, url = url.split(":", 1)
        if  not self._headers.get("suppress_origin"):

            origin = self._headers.get("origin")

            if origin:
                headers.append(f"Origin: {origin}")

            elif self.url.scheme == "wss":
                headers.append(f"Origin: https://{hostport}")
                
            else:
                headers.append(f"Origin: http://{hostport}")

        key = create_sec_websocket_key()

        header = self._headers.get("header")
        if not header or 'Sec-WebSocket-Key' not in header:
            headers.append(f"Sec-WebSocket-Key: {key}")
        else:
            key = self._headers.get("header", {}).get('Sec-WebSocket-Key')

        if not header or 'Sec-WebSocket-Version' not in header:
            headers.append(f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}")

        connection = self._headers.get('connection')
        if not connection:
            headers.append('Connection: Upgrade')
        else:
            headers.append(connection)

        subprotocols = self._headers.get("subprotocols")
        if subprotocols:
            headers.append("Sec-WebSocket-Protocol: %s" % ",".join(subprotocols))

        if header:
            if isinstance(header, dict):
                header = [
                    ": ".join([k, v])
                    for k, v in header.items()
                    if v is not None
                ]
            headers.extend(header)

        headers.append("")
        headers.append("")

        self.encoded_headers = '\r\n'.join(headers).encode()