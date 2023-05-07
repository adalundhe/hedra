import os
import re
import uuid
import json
import asyncio
import functools
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
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.stages.base.stage import Stage
from hedra.plugins.types.extension.types import ExtensionType
from hedra.plugins.types.extension import (
    ExtensionPlugin,
    execute,
    prepare
)
from hedra.versioning.flags.types.unstable.flag import unstable
from typing import Dict, List

try:

    har_extension_enabled=True

    from haralyzer import HarParser, HarEntry
    from haralyzer.http import Request

except ImportError:
    har_extension_enabled=False

    HarParser=object
    HarEntry=object
    Request=object


@unstable
class HarConverter(ExtensionPlugin):
    extension_type = ExtensionType.GENERATOR

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop = None
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')
        self._parser: HarParser = None
        self._action_data: List[ActionHook] = []
        self.name = HarConverter.__name__
        self.extension_type = self.extension_type
        
    @prepare()
    async def load(
        self,
        persona_config: Config=None,
        execute_stage: Stage=None
    ) -> Dict[str, List[ActionHook]]:

        self._loop = asyncio.get_event_loop()
        await self._load_harfile(
            persona_config,
            execute_stage
        )

        return await self._to_actions(
            persona_config,
            execute_stage
        )
    
    @execute()
    async def convert(
        self,
        action_data: List[ActionHook]=[],
        execute_stage: Stage=None
    ) -> Dict[str, Stage]:

        action_hooks = execute_stage.hooks[HookType.ACTION]

        if len(execute_stage.hooks[HookType.ACTION]) > 0:
            max_existing_hook_order = max([
                hook.order for hook in action_hooks
            ])

        else:
            max_existing_hook_order = 0

        sequence_order = max_existing_hook_order + 1

        for hook in action_data:
            hook.order = sequence_order

            sequence_order += 1

        action_hooks.extend(action_data)

        execute_stage.hooks[HookType.ACTION] = action_hooks

        return {
            'execute_stage': execute_stage
        }

    async def _load_harfile(
        self,
        config: Config,
        execute_stage: Stage
    ) -> None:

        har_filepath = await self._loop.run_in_executor(
            None,
            functools.partial(
                os.path.abspath,
                config.actions_filepaths.get(execute_stage.name)
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

        await self._loop.run_in_executor(
            None,
            harfile.close
        )

    async def _to_actions(
        self,
        config: Config,
        execute_stage: Stage=None
    ) -> List[ActionHook]:
        
        action_data: List[ActionHook] = []

        for page in self._parser.pages:
            
            page_entries: List[HarEntry] = page.entries

            for entry in page_entries:
                
                page_request: Request = entry.request

                action_data = page_request.text
                
                content_type: str = page_request.mimeType
                if content_type.lower() == 'application/json' and page_request.text:
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

                action_fullname = f'{action_method.lower()}_{action_basename}'

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
                        concurrency=config.batch_size,
                        timeouts=Timeouts(
                            connect_timeout=config.connect_timeout,
                            total_timeout=config.request_timeout
                        ),
                        reset_connections=config.reset_connections,
                        tracing_session=config.tracing
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
                        concurrency=config.batch_size,
                        timeouts=Timeouts(
                            connect_timeout=config.connect_timeout,
                            total_timeout=config.request_timeout
                        ),
                        reset_connections=config.reset_connections,
                        tracing_session=config.tracing
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
                        concurrency=config.batch_size,
                        timeouts=Timeouts(
                            connect_timeout=config.connect_timeout,
                            total_timeout=config.request_timeout
                        ),
                        reset_connections=config.reset_connections,
                        tracing_session=config.tracing
                    )

                await action.url.lookup()
                action.setup()

                hook = ActionHook(
                    f'{execute_stage.name}.{action_basename}',
                    action_basename,
                    None
                )

                hook.session = session
                hook.action = action
                hook.stage = execute_stage.name
                hook.stage_instance = execute_stage
                hook.context = SimpleContext()
                hook.hook_id = uuid.uuid4()

                action_data.append(hook)

            return {
                'action_data': action_data
            }
        