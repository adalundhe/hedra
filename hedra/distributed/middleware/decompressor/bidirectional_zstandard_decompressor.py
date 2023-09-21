import zstandard
from hedra.distributed.middleware.base import (
    Middleware,
    MiddlewareType
)
from hedra.distributed.models.http import (
    Request,
    Response
)
from pydantic import BaseModel
from typing import (
    Dict, 
    Union,
    Tuple,
    Callable
)


class BidirectionalZStandardDecompressor(Middleware):

    def __init__(
        self, 
        compression_level: int=9,
        serializers: Dict[
            str,
            Callable[
                [
                    Union[
                        Response,
                        BaseModel,
                        str,
                        None
                    ]
                ],
                Union[
                    str,
                    None
                ]
            ]
        ]={}
    ) -> None:
        super().__init__(
            self.__class__.__name__,
            middleware_type=MiddlewareType.BIDIRECTIONAL
        )

        self.compression_level = compression_level
        self.serializers = serializers
        self._decompressor = zstandard.ZstdDecompressor()

    async def __pre__(
        self,
        request: Request,
        response: Union[
            BaseModel,
            str,
            None
        ],
        status: int
    ):
        try:

            headers = request.headers
            content_encoding = headers.get(
                'content-encoding',
                headers.get('x-compression-encoding')
            )
           
            if request.raw != b'' and content_encoding == 'gzip':

                request.content = self._decompressor.decompress(
                    request.content
                )

                request_headers = {
                    key: value for key, value in headers.items() if key != 'content-encoding' and key != 'x-compression-encoding'
                }


            return (
                request,
                Response(
                    request.path,
                    request.method,
                    headers=request_headers
                ),
                200
            ), True

        except Exception as e:
            return (
                None,
                Response(
                    request.path,
                    request.method,
                    data=str(e)
                ),
                500
            ), False
        
    async def __post__(
        self, 
        request: Request,
        response: Union[
            Response,
            BaseModel,
            str,
            None
        ],
        status: int
    ) -> Tuple[
        Tuple[Response, int], 
        bool
    ]:
        try:
            
            if response is None:
                return (
                    request,
                    Response(
                        request.path,
                        request.method,
                        data=response
                    ),
                    status
                ), True
            
            elif isinstance(response, str):

                decompressed_data = self._decompressor.decompress(
                    response.encode()
                )

                return (
                    request,
                    Response(
                        request.path,
                        request.method,
                        headers={
                            'x-compression-encoding': 'gzip',
                            'content-type': 'text/plain'
                        },
                        data=decompressed_data.decode()
                    ),
                    status
                ), True
            
            else:

                headers = response.headers
                content_encoding = headers.get(
                    'content-encoding',
                    headers.get('x-compression-encoding')
                )

                if content_encoding == 'gzip':

                    headers.pop(
                        'content-encoding',
                        headers.pop(
                            'x-compression-encoding',
                            None
                        )
                    )

                    serialized = self.serializers[request.path](response)
                    decompressed_data = self._decompressor.decompress(
                        serialized
                    )

                    return (
                        request,
                        Response(
                            request.path,
                            request.method,
                            headers=headers,
                            data=decompressed_data.decode()
                        ),
                        status
                    ), True
                
                return (
                    response,
                    status
                ), True
            
        except KeyError:
            return (
                request,
                Response(
                    request.path,
                    request.method,
                    data=f'No serializer for {request.path} found.'
                ),
                500
            ), False

        except Exception as e:
            return (
                request,
                Response(
                    request.path,
                    request.method,
                    data=str(e)
                ),
                500
            ), False