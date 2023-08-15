import functools


def server():

    def wraps(func):

        func.server_only = True
        func.as_http = False

        @functools.wraps(func)
        def decorator(
            *args,
            **kwargs
        ):
            return func(*args, **kwargs)
        
        return decorator
    
    return wraps
