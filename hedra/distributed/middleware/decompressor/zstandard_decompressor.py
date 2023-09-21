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


class ZStandardDecompressor(Middleware):

    def __init__(
        self, 
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
            middleware_type=MiddlewareType.UNIDIRECTIONAL_AFTER
        )

        self.serializers = serializers
        self._decompressor = zstandard.ZstdDecompressor()

    async def __run__(
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