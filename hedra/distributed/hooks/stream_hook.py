
            

import functools
from hedra.distributed.service import Service
from hedra.distributed.service.controller import Controller
from typing import Union


def stream(
    call_name: str, 
    as_tcp: bool=False
):

    def wraps(func):

        func.client_only = True
        func.target = call_name

        @functools.wraps(func)
        async def decorator(
            *args,
            **kwargs
        ):
            connection: Union[Service, Controller] = args[0]

            if as_tcp:

                async for data in func(*args, **kwargs):
                    async for response in connection.stream_tcp(
                        call_name,
                        data
                    ):
                        yield response


            else:
                async for data in func(*args, **kwargs):
                    async for response in connection.stream(
                        call_name,
                        data
                    ):

                        yield response
            
        return decorator
    
    return wraps
