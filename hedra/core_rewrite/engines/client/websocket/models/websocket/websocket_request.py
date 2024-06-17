from typing import Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlencode

import orjson
from pydantic import BaseModel, StrictBytes, StrictInt, StrictStr

from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    HTTPCookie,
    HTTPEncodableValue,
)

from .constants import WEBSOCKETS_VERSION
from .utils import create_sec_websocket_key, pack_hostname

NEW_LINE = "\r\n"


class WebsocketRequest(BaseModel):
    url: StrictStr
    method: Literal["GET", "POST", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"]
    cookies: Optional[List[HTTPCookie]] = None
    auth: Optional[Tuple[str, str]] = None
    params: Optional[Dict[str, HTTPEncodableValue]] = None
    headers: Dict[str, str] = {}
    data: Union[Optional[StrictStr], Optional[StrictBytes], Optional[BaseModel]] = None
    redirects: StrictInt = 3

    class Config:
        arbitrary_types_allowed = True

    def prepare(self, url: URL):
        url_path = url.path

        if self.params and len(self.params) > 0:
            url_params = urlencode(self.params)
            url_path += f"?{url_params}"

        get_base: str = f"{self.method} {url.path} HTTP/1.1{NEW_LINE}"

        port = url.port or (443 if url.scheme == "https" else 80)

        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f"{hostname}:{port}"

        if isinstance(self.data, BaseModel):
            data = orjson.dumps(
                {
                    name: value
                    for name, value in self.data.__dict__.items()
                    if value is not None
                }
            )
            self.headers["content-type"] = "application/json"

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

        headers = ["Upgrade: websocket", f"Content-Length: {size}"]

        if url.port == 80 or url.port == 443:
            hostport = pack_hostname(url.hostname)
        else:
            hostport = "%s:%d" % (pack_hostname(url.hostname), url.port)

        host = self.headers.get("host")
        if host:
            headers.append(f"Host: {host}")
        else:
            headers.append(f"Host: {hostport}")

        # scheme, url = url.split(":", 1)
        if not self.headers.get("suppress_origin"):
            origin = self.headers.get("origin")

            if origin:
                headers.append(f"Origin: {origin}")

            elif url.scheme == "wss":
                headers.append(f"Origin: https://{hostport}")

            else:
                headers.append(f"Origin: http://{hostport}")

        key = create_sec_websocket_key()

        header = self.headers.get("header")
        if not header or "Sec-WebSocket-Key" not in header:
            headers.append(f"Sec-WebSocket-Key: {key}")
        else:
            key = self.headers.get("header", {}).get("Sec-WebSocket-Key")

        if not header or "Sec-WebSocket-Version" not in header:
            headers.append(f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}")

        connection = self.headers.get("connection")
        if not connection:
            headers.append("Connection: Upgrade")
        else:
            headers.append(connection)

        subprotocols = self.headers.get("subprotocols")
        if subprotocols:
            headers.append("Sec-WebSocket-Protocol: %s" % ",".join(subprotocols))

        if header:
            if isinstance(header, dict):
                header = [": ".join([k, v]) for k, v in header.items() if v is not None]

            headers.extend(header)

        headers.append("")
        headers.append("")

        get_base += "\r\n".join(headers).encode()

        return (get_base + NEW_LINE).encode(), data
