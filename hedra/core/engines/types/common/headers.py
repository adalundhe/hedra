import os
from base64 import encodebytes as base64encode
from typing import Dict
from typing import Union
from .encoder import Encoder
from hedra.core.engines.types.common.params import Params
from .constants import NEW_LINE, WEBSOCKETS_VERSION
from .url import URL




def _create_sec_websocket_key():
    randomness = os.urandom(16)
    return base64encode(randomness).decode('utf-8').strip()

def _pack_hostname(hostname):
    # IPv6 address
    if ':' in hostname:
        return '[' + hostname + ']'

    return hostname


class Headers:

    def __init__(self, headers: Dict[str, str]) -> None:
        self.data = headers
        self.encoded_headers = None
        self.hpack_encoder = Encoder()
        self.headers_setup = False

    @property
    def as_dict(self):
        return self.data

    def __getitem__(self, header_name: str) -> Union[str, str]:
        return self.data.get(header_name, "").lower()

    def __setitem__(self, header_name: str, header_value: str) -> None:
        header_name = header_name.lower()
        self.data[header_name] = header_value
        self.headers_setup = False

    def setup_http_headers(self, method: str, url: URL,  params: Params) -> Union[bytes, Dict[str, str]]:
            path = url.path
            
            if url.query:
                path = f'{path}?{url.query}'

            elif params.has_params:
                path = f'{path}{params.encoded}'


            get_base = f"{method.upper()} {path} HTTP/1.1{NEW_LINE}"
            port = url.port or (443 if url.scheme == "https" else 80)
            hostname = url.parsed.hostname.encode("idna").decode()

            if port not in [80, 443]:
                hostname = f'{hostname}:{port}'

            self.data = {
                "HOST": hostname,
                "User-Agent": "mercury-http",
                **self.data
            }

            for key, value in self.data.items():
                get_base += f"{key}: {value}{NEW_LINE}"

            self.encoded_headers = (get_base + NEW_LINE).encode()
            self.headers_setup = True


    def setup_http2_headers(self, method: str, url: URL) -> None:
        
        self.encoded_headers = [
            (b":method", method),
            (b":authority", url.authority),
            (b":scheme", url.scheme),
            (b":path", url.path),
        ]

        self.encoded_headers.extend([
            (
                k.lower(), 
                v
            )
            for k, v in self.data.items()
            if k.lower()
            not in (
                b"host",
                b"transfer-encoding",
            )
        ])
        
        self.encoded_headers = self.hpack_encoder.encode(self.encoded_headers)
        self.data.update({
            "authority": url.authority,
            "scheme": url.scheme,
            "path": url.path,
            "method": method
        })

        self.headers_setup = True

    def setup_websocket_headders(self, method: str, url: URL):

        for header_name, header_value in self.data.items():
            header_name_lowered = header_name.lower()
            self.data[header_name_lowered] = header_value

        headers = [
            "GET %s HTTP/1.1" % url.path,
            "Upgrade: websocket"
        ]
        if url.port == 80 or url.port == 443:
            hostport = _pack_hostname(url.hostname)
        else:
            hostport = "%s:%d" % (_pack_hostname(url.hostname), url.port)

        host = self.data.get("host")
        if host:
            headers.append(f"Host: {host}")
        else:
            headers.append(f"Host: {hostport}")

        # scheme, url = url.split(":", 1)
        if  not self.data.get("suppress_origin"):

            origin = self.data.get("origin")

            if origin:
                headers.append(f"Origin: {origin}")

            elif url.scheme == "wss":
                headers.append(f"Origin: https://{hostport}")
                
            else:
                headers.append(f"Origin: http://{hostport}")

        key = _create_sec_websocket_key()

        header = self.data.get("header")
        if not header or 'Sec-WebSocket-Key' not in header:
            headers.append(f"Sec-WebSocket-Key: {key}")
        else:
            key = self.data.get("header", {}).get('Sec-WebSocket-Key')

        if not header or 'Sec-WebSocket-Version' not in header:
            headers.append(f"Sec-WebSocket-Version: {WEBSOCKETS_VERSION}")

        connection = self.data.get('connection')
        if not connection:
            headers.append('Connection: Upgrade')
        else:
            headers.append(connection)

        subprotocols = self.data.get("subprotocols")
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
        self.headers_setup = True

    def setup_graphql_headers(self, url: URL, params: Params, use_http2=False):
        self.data['Content-Type'] = "application/json"

        if use_http2:
            self.setup_http2_headers("POST", url)

        else:
            self.setup_http_headers("POST", url, params)

    def setup_grpc_headers(self, url: URL, timeout=60):
        grpc_headers = {
            'Content-Type': 'application/grpc',
            'Grpc-Timeout': f'{timeout}S',
            'TE': 'trailers'
        }
        self.data.update(grpc_headers)

        self.setup_http2_headers("POST", url)
