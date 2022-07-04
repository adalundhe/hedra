import functools

def after(*names):
    
    def wrapper(func):
        func.names = names
        func.is_action = True
        func.is_before = False
        func.is_after = True
        func.is_setup = False
        func.is_teardown = False

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper