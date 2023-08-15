import json
from pydantic import (
    StrictStr,
    StrictInt,
    Json
)
from typing import Dict, Optional, Union, Literal, List
from .message import Message


class HTTPMessage(Message):
    protocol: StrictStr="HTTP/1.1"
    path: Optional[StrictStr]
    method: Optional[
        Literal[
            "GET", 
            "POST",
            "HEAD",
            "OPTIONS", 
            "PUT", 
            "PATCH", 
            "DELETE"
        ]
    ]
    status: Optional[StrictInt]
    status_message: Optional[StrictStr]
    params: Dict[StrictStr, StrictStr]={}
    headers: Dict[StrictStr, StrictStr]={}
    data: Optional[Union[Json, StrictStr]]

    def prepare_response(self):

        message = 'OK'
        if self.error:
            message = self.error

        head_line = f'HTTP/1.1 {self.status} {message}'
            
        encoded_data: str = ''

        if isinstance(self.data, Message):
            encoded_data = json.dumps(self.data.to_data())

            content_length = len(encoded_data)
            headers = f'content-length: {content_length}'

        elif self.data:
            encoded_data = self.data

            content_length = len(encoded_data)
            headers = f'content-length: {content_length}'

        else:
            headers = 'content-length: 0'

        response_headers = self.headers
        if response_headers:
            for key in response_headers:
                headers = f'{headers}\r\n{key}: {response_headers[key]}'

        return f'{head_line}\r\n{headers}\r\n\r\n{encoded_data}'.encode()