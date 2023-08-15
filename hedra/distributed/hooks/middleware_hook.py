import functools


def middleware():

    def wraps(func):

        func.is_middleware = True

        @functools.wraps(func)
        def decorator(
            *args,
            **kwargs
        ):
            return func(*args, **kwargs)
        
        return decorator
    
    return wraps
