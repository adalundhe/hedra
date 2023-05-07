import os
import re
import json
import asyncio
import functools
from haralyzer import HarParser, HarEntry
from haralyzer.http import Request
from hedra.core.hooks.types.action.hook import Hook
from hedra.core.engines.types.http import HTTPAction
from hedra.core.engines.types.http2 import HTTP2Action
from hedra.core.engines.types.http3 import HTTP3Action
from hedra.plugins.types.extension import (
    ExtensionPlugin,
    execute
)
from hedra.versioning.flags.types.unstable.flag import unstable
from typing import (
    Dict, 
    Any, 
    List,
    Union
)


@unstable
class HarConverter(ExtensionPlugin):

    def __init__(
        self,
        filepath: str
    ) -> None:
        self.filepath = os.path.abspath(filepath)
        self._har_data: HarParser = None
        self._loop: asyncio.AbstractEventLoop = None
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')

    @execute()
    async def convert(self):

        self._loop = asyncio.get_event_loop()

        har_parser = await self._load_harfile()
        action_data = await self._to_actions(har_parser)

    async def _load_harfile(self) -> HarParser:

        harfile = await self._loop.run_in_executor(
            None,
            functools.partial(
                self.filepath
            )
        )

        return HarParser(
            har_data=json.loads(harfile)
        )

    async def _to_actions(
        self,
        har_parser: HarParser
    ) -> List[Union[HTTPAction, HTTP2Action, HTTP3Action]]:
        
        action_data: List[Union[HTTPAction, HTTP2Action, HTTP3Action]] = []
        sequence_order = 0

        for page in har_parser.pages:
            
            page_entries: List[HarEntry] = page.entries

            for entry in page_entries:
                
                page_request: Request = entry.request

                action_data = page_request.text
                
                content_type: str = page_request.mimeType
                if content_type.lower() == 'application/json':
                    action_data = json.loads(action_data)

                action_url: str = page_request.url
                action_method_stub: str= page_request.method
                action_method = action_method_stub.upper()
                action_headers: List[Dict[str, str]] = page_request.headers

                action_basename = '_'.join([
                    segment.capitalize() for segment in self._name_pattern.sub(
                        '_', 
                        action_url
                    ).split('_')
                ])

                action_fullname = f'{action_method}_{action_basename}'

                http_type: str = page_request.httpVersion

                if http_type == "http/3.0":

                    action = HTTP3Action(
                        action_fullname,
                        action_url,
                        method=action_method,
                        headers={
                            header["name"]: header["value"] for header in action_headers
                        },
                        data=action_data
                    )

                elif http_type == "http/2.0":
                    action = HTTP2Action(
                        action_fullname,
                        action_url,
                        method=action_method,
                        headers={
                            header["name"]: header["value"] for header in action_headers
                        },
                        data=action_data
                    )

                else:
                    action = HTTPAction(
                        action_fullname,
                        action_url,
                        method=action_method,
                        headers={
                            header["name"]: header["value"] for header in action_headers
                        },
                        data=action_data
                    )

                await action.url.lookup()
                action.setup()

                action_data.append(action)

            return action_data
        