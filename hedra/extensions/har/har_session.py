import os
import json
from haralyzer import HarParser, HarEntry
from haralyzer.http import Request
from hedra.core.engines.types.common.types import RequestTypes
from typing import Dict, Any, List


class HarSession:

    def __init__(
        self,
        filepath: str
    ) -> None:
        self.filepath = os.path.abspath(filepath)
        self._har_data: HarParser = None
        self._action_data: List[Dict[str, Any]] = []

    def load_harfile(self):

        with open(self.filepath) as harfile:
            self._har_data = HarParser(
                har_data=json.loads(harfile)
            )

    def to_actions(self):

        for page in self._har_data.pages:
            
            page_entries: List[HarEntry] = page.entries

            for entry in page_entries:
                
                page_request: Request = entry.request

                data = page_request.text
                
                content_type: str = page_request.mimeType

                if content_type.lower() == 'application/json':
                    data = json.loads(data)

                http_type: str = page_request.httpVersion

                if http_type == "http/3.0":
                    request_type = RequestTypes.HTTP3

                elif http_type == "http/2.0":
                    request_type = RequestTypes.HTTP2

                else:
                    request_type = RequestTypes.HTTP

                self._action_data.append({
                    "request_type": request_type,
                    'url': page_request.url,
                    'method': page_request.method,
                    'headers': page_request.headers,
                    'data': data
                    
                })
        