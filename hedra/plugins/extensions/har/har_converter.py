import os
import re
import json
import asyncio
import functools
from haralyzer import HarParser, HarEntry
from haralyzer.http import Request
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http import (
    HTTPAction,
    MercuryHTTPClient
)
from hedra.core.engines.types.http2 import (
    HTTP2Action,
    MercuryHTTP2Client
)
from hedra.core.engines.types.http3 import (
    HTTP3Action,
    MercuryHTTP3Client
)
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.graphs.stages.base.stage import Stage
from hedra.plugins.types.extension import (
    ExtensionPlugin,
    execute,
    prepare
)
from hedra.versioning.flags.types.unstable.flag import unstable
from typing import Dict, List


@unstable
class HarConverter(ExtensionPlugin):

    def __init__(
        self,
        filepath: str=None
    ) -> None:
        self.filepath: str = filepath
        self._loop: asyncio.AbstractEventLoop = None
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')
        self._config: Config = None
        self._parser: HarParser = None
        self._action_data: List[ActionHook] = []

    @property
    def name(self):
        return HarConverter.__name__
    
    @prepare()
    async def load(
        self,
        filepath: str=None,
        persona_config: Config=None
    ) -> Dict[str, List[ActionHook]]:
        
        if persona_config:
            self._config = persona_config

        self._loop = asyncio.get_event_loop()

        await self._load_harfile(
            filepath=filepath
        )

        return await self._to_actions()
    
    @execute()
    async def convert(
        self,
        action_data: List[ActionHook]=[],
        execute_stage: Stage=None
    ) -> Dict[str, Stage]:

        action_hooks = execute_stage.hooks[HookType.ACTION]

        max_existing_hook_order = max([
            hook.order for hook in action_hooks
        ])

        for hook in action_data:
            hook.order += max_existing_hook_order

        action_hooks.extend(action_data)

        execute_stage.hooks[HookType.ACTION] = action_hooks

        return {
            'execute_stage': execute_stage
        }

    async def _load_harfile(
        self,
        filepath: str=None
    ) -> None:
        
        if filepath is None:
            filepath = self.filepath

        har_filepath = await self._loop.run_in_executor(
            None,
            functools.partial(
                os.path.abspath,
                filepath
            )
        )

        harfile = await self._loop.run_in_executor(
            None,
            functools.partial(
                open,
                har_filepath
            )
        )

        self._parser = HarParser(
            har_data=json.loads(harfile)
        )

    async def _to_actions(self) -> List[ActionHook]:
        
        action_data: List[ActionHook] = []
        sequence_order = 0

        for page in self._parser.pages:
            
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
                    
                    session = MercuryHTTP3Client(
                        concurrency=self.config.batch_size,
                        timeouts=Timeouts(
                            connect_timeout=self.config.connect_timeout,
                            total_timeout=self.config.request_timeout
                        ),
                        reset_connections=self.config.reset_connections,
                        tracing_session=self.config.tracing
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

                    session = MercuryHTTP2Client(
                        concurrency=self.config.batch_size,
                        timeouts=Timeouts(
                            connect_timeout=self.config.connect_timeout,
                            total_timeout=self.config.request_timeout
                        ),
                        reset_connections=self.config.reset_connections,
                        tracing_session=self.config.tracing
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

                    session = MercuryHTTPClient(
                        concurrency=self.config.batch_size,
                        timeouts=Timeouts(
                            connect_timeout=self.config.connect_timeout,
                            total_timeout=self.config.request_timeout
                        ),
                        reset_connections=self.config.reset_connections,
                        tracing_session=self.config.tracing
                    )

                await action.url.lookup()
                action.setup()

                hook = ActionHook(
                    f'{HarConverter.__name__}.{action_basename}',
                    action_basename,
                    None,
                    order=sequence_order
                )

                hook.session = session
                hook.action = action
                hook.stage = self.name

                action_data.append(hook)
                sequence_order += 1

            return {
                'action_data': action_data
            }
        