import asyncio
import uuid
from typing import (
    Optional,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

from hedra.core_rewrite.engines.client.client_types.common import Timeouts
from hedra.core_rewrite.engines.client.http2 import MercurySyncHTTP2Connection
from hedra.core_rewrite.engines.client.shared.models import (
    Metadata,
    URLMetadata,
)

from .models.grpc import (
    GRPCRequest,
    GRPCResponse,
)

T = TypeVar('T')

class MercurySyncGRPCConnection(MercurySyncHTTP2Connection):

    def __init__(
        self, 
        pool_size: int = 10 ** 3, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool=False,
    ) -> None:
        super(
            MercurySyncGRPCConnection,
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
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        redirects: int=3
    ) -> GRPCResponse:
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        GRPCRequest(
                            url=url,
                            protobuf=protobuf,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return GRPCResponse(
                    metadata=Metadata(),
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    status=408,
                    status_message='Request timed out.'
                )
