from base64 import b64encode
from gzip import compress
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


class GZipCompressor(Middleware):

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
            middleware_type=MiddlewareType.UNIDIRECTIONAL_AFTER
        )

        self.compression_level = compression_level
        self.serializers = serializers

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
                    Response(
                        request.path,
                        request.method,
                        data=response
                    ),
                    status
                ), True
            
            elif isinstance(response, str):
           
                compressed_data = compress(
                    response.encode(),
                    compresslevel=self.compression_level
                )


                return (
                    Response(
                        request.path,
                        request.method,
                        headers={
                            'content-encoding': 'gzip'
                        },
                        data=b64encode(compressed_data).decode()
                    ),
                    status
                ), True
            
            else:
                serialized = self.serializers[request.path](response)
                
                compressed_data = compress(
                    serialized,
                    compresslevel=self.compression_level
                )

                response.headers.update({
                    'x-compression-encoding': 'gzip',
                    'content-type': 'text/plain'
                })

                return (
                    Response(
                        request.path,
                        request.method,
                        headers=response.headers,
                        data=b64encode(compressed_data).decode()
                    ),
                    status
                ), True
            
        except KeyError:
            return (
                Response(
                    request.path,
                    request.method,
                    data=f'No serializer for {request.path} found.'
                ),
                500
            ), False

        except Exception as e:
            return (
                Response(
                    request.path,
                    request.method,
                    data=str(e)
                ),
                500
            ), False