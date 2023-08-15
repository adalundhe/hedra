import functools
from hedra.distributed.service import Service
from hedra.distributed.service.controller import Controller
from typing import Union


def client(
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
                return await connection.send_tcp(
                    call_name,
                    await func(*args, **kwargs)
                )

            else:
                return await connection.send(
                    call_name,
                    await func(*args, **kwargs)
                )

        return decorator
    
    return wraps
