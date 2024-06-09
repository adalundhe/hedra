import asyncio
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from hedra.core_rewrite.engines.client.client_types.common import Timeouts
from hedra.core_rewrite.engines.client.http import MercurySyncHTTPConnection

from .models.graphql import (
    GraphQLRequest,
    GraphQLResponse,
    HTTPCookie,
    HTTPEncodableValue,
    Metadata,
    URLMetadata,
)


class MercurySyncGraphQLConnection(MercurySyncHTTPConnection):

    def __init__(
        self, 
        pool_size: int = 10 ** 3, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool = False
    ) -> None:

        super(
            MercurySyncGraphQLConnection,
            self
        ).__init__(
            pool_size=pool_size, 
            cert_path=cert_path,
            key_path=key_path,
            timeouts=timeouts, 
            reset_connections=reset_connections
        )

        self.session_id = str(uuid.uuid4())
        
    async def query(
        self,
        url: str,
        query: str,
        operation_name: str = None,
        variables: Dict[str, Any] = None, 
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        
        
        redirects: int=3
    ) -> GraphQLResponse:
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        GraphQLRequest(
                            url=url,
                            method='POST',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            data={
                                "query": query,
                                "operation_name": operation_name,
                                "variables": variables
                            },
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return GraphQLResponse(
                    metadata=Metadata(),
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