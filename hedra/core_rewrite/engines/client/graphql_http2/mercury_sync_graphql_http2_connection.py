import asyncio
import uuid
from typing import (
    Dict,
    Optional,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

from hedra.core_rewrite.engines.client.http2 import MercurySyncHTTP2Connection
from hedra.core_rewrite.engines.client.shared.models import URLMetadata
from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.graphql_http2 import (
    GraphQLHTTP2Request,
    GraphQLHTTP2Response,
)

T = TypeVar('T')


class MercurySyncGraphQLHTTP2Connection(MercurySyncHTTP2Connection):

    def __init__(
        self, 
        pool_size: int = 10 ** 3, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool = False
    ) -> None:

        super(
            MercurySyncGraphQLHTTP2Connection, 
            self
        ).__init__(
            pool_size=pool_size, 
            cert_path=cert_path,
            key_path=key_path,
            timeouts=timeouts, 
            reset_connections=reset_connections
        )

        self.session_id = str(uuid.uuid4())

    async def send(
        self,
        url: str,
        protobuf: T,
        headers: Dict[str, str]={},
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        redirects: int=3
    ) -> GraphQLHTTP2Response:
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        GraphQLHTTP2Request(
                            url=url,
                            method='POST',
                            headers=headers,
                            protobuf=protobuf,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return GraphQLHTTP2Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='POST',
                    status=408,
                    status_message='Request timed out.'
                )
  