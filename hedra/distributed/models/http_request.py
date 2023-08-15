import json
from enum import Enum
from pydantic import AnyHttpUrl
from typing import Dict, Optional, List, Union
from urllib.parse import urlparse
from .http_message import HTTPMessage
from .message import Message


class HTTPRequestMethod(Enum):
    GET='GET'
    POST='POST'


class HTTPRequest(Message):
    url: AnyHttpUrl
    method: HTTPRequestMethod
    params: Optional[Dict[str, str]]
    headers: Dict[str, str]={}
    data: Optional[Union[str, Message]]

    class Config:
        arbitrary_types_allowed=True

    def prepare_request(self):
        parsed = urlparse(self.url)

        path = parsed.path
        if path is None:
            path = "/"


        if self.params:
            
            params_string = '&'.join([
                f'{name}={value}' for name, value in self.params
            ])

            path = f'{path}?{params_string}'

        request: List[str] = [
            f'{self.method.value} {path} HTTP/1.1'
        ]

        request.append(
            f'host: {parsed.hostname}'
        )

        request.extend([
            f'{key}: {value}' for key, value in self.headers.items()
        ])

        encoded_data = None
        if isinstance(self.data, Message):
            encoded_data = json.dumps(self.data.to_data())

            request.append(
                'content-type: application/msync'
            )

        elif self.data:
            encoded_data = self.data
            content_length = len(encoded_data)
            
            request.append(
                f'content-length: {content_length}'
            )

        request.append('\r\n')

        if encoded_data:
            request.append(encoded_data)

        encoded_request = '\r\n'.join(request)


        return encoded_request.encode()
    
    @classmethod
    def parse(cls, data: bytes):
        response = data.split(b'\r\n')
        
        response_line = response[0]

        headers: Dict[bytes, bytes] = {}

        header_lines = response[1:]
        data_line_idx = 0

        for header_line in header_lines:

            if header_line == b'':
                data_line_idx += 1
                break
            
            key, value = header_line.decode().split(
                ':', 
                maxsplit=1
            )
            headers[key.lower()] = value.strip()

            data_line_idx += 1

        data = b''.join(response[data_line_idx + 1:]).strip()
        

        request_type, status, message = response_line.decode().split(' ')

        return HTTPMessage(
            protocol=request_type,
            status=int(status),
            status_message=message,
            headers=headers,
            data=data.decode()
        )
    
    @classmethod
    def parse_request(cls, data: bytes):
        response = data.split(b'\r\n')
        
        response_line = response[0]

        headers: Dict[bytes, bytes] = {}

        header_lines = response[1:]
        data_line_idx = 0

        for header_line in header_lines:

            if header_line == b'':
                data_line_idx += 1
                break
            
            key, value = header_line.decode().split(
                ':', 
                maxsplit=1
            )
            headers[key.lower()] = value.strip()

            data_line_idx += 1

        data = b''.join(response[data_line_idx + 1:]).strip()
        
        method, path, request_type = response_line.decode().split(' ')

        if path is None or path == '':
            path = "/"

        return HTTPMessage(
            method=method,
            path=path,
            protocol=request_type,
            headers=headers,
            data=data.decode()
        )
