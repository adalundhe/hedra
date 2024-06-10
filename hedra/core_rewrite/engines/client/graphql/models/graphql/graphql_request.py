from typing import Dict, List, Literal, Optional

import orjson
from pydantic import BaseModel, StrictInt, StrictStr

from hedra.core_rewrite.engines.client.shared.models import URL, HTTPCookie

try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source=None
    parse=lambda: None
    print_ast=lambda: None


NEW_LINE = '\r\n'


class GraphQLRequest(BaseModel):
    url: StrictStr
    method: Literal[
        "GET", 
        "POST",
    ]
    cookies: Optional[List[HTTPCookie]]=None
    headers: Dict[str, str]={}
    data: Dict[
        Literal[
            'query', 
            'operation_name', 
            'variables'
        ], 
        str
    ]={}
    redirects: StrictInt=3

    class Config:
        arbitrary_types_allowed=True

    def prepare(
        self,
        url: URL
    ):

        get_base = f"{self.method} {url.path} HTTP/1.1{NEW_LINE}"

        port = url.port or (443 if url.scheme == "https" else 80)

        hostname = url.hostname.encode("idna").decode()

        if port not in [80, 443]:
            hostname = f'{hostname}:{port}'

        source = Source(self.data.get("query"))
        document_node = parse(source)
        query_string = print_ast(document_node)
        
        size = len(query_string)
        
        query = {
            "query": query_string
        }

        operation_name = self.data.get("operation_name")
        variables = self.data.get("variables")
        
        if operation_name:
            query["operationName"] = operation_name
        
        if variables:
            query["variables"] = variables

        data = orjson.dumps(query).encode()

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
