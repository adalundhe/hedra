from typing import Dict, Iterator, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http import (
    MercuryHTTPClient,
    HTTPAction,
    HTTPResult
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.core.engines.types.tracing.trace_session import (
    TraceSession, 
    Trace
)
from hedra.logging import HedraLogger
from .base_client import BaseClient


class HTTPClient(BaseClient[MercuryHTTPClient, HTTPAction, HTTPResult]):

    def __init__(self, config: Config) -> None:
        super().__init__()

        if config is None:
            config = Config()

        tracing_session: Union[TraceSession, None] = None
        if config.tracing:
            trace_config_dict = config.tracing.to_dict()
            tracing_session = TraceSession(**trace_config_dict)

        self.session = MercuryHTTPClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections,
            tracing_session=tracing_session
        )
        self.request_type = RequestTypes.HTTP
        self.client_type = self.request_type.capitalize()

        self.next_name = None
        self.intercept = False
        self.waiter = None
        self.actions: ActionsStore = None
        self.registered = {}

        self.logger = HedraLogger()
        self.logger.initialize()



    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def get(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3,
        trace: Trace=None
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTPAction(
            self.next_name,
            url,
            method='GET',
            headers=headers,
            data=None,
            user=user,
            tags=tags,
            redirects=redirects             
        )

        return await self._execute_action(request)

    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3,
        trace: Trace=None
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTPAction(
            self.next_name,
            url,
            method='POST',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects           
        )

        return await self._execute_action(request)

    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3,
        trace: Trace=None
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTPAction(
            self.next_name,
            url,
            method='PUT',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects
        )

        return await self._execute_action(request)

    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3,
        trace: Trace=None
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTPAction(
            self.next_name,
            url,
            method='PATCH',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects
        )

        return await self._execute_action(request)

    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3,
        trace: Trace=None
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTPAction(
            self.next_name,
            url,
            method='DELETE',
            headers=headers,
            data=None,
            user=user,
            tags=tags,
            redirects=redirects
        )

        return await self._execute_action(request)
