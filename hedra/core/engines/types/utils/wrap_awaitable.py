import time
import functools
import traceback

def async_execute_or_catch():

    def wrapper(func):

        @functools.wraps(func)
        async def execute(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
                
            except Exception as execution_error:
                return {
                    'response': None,
                    'error': execution_error
                }

        return execute
    return wrapper



async def wrap_awaitable_future(action, context):
    start = time.time()
    try:
        response = await action.execute(context)
        return {
            'total_time': time.time() - start,
            'response': response,
            **action.to_dict()
        }
    except Exception as execution_error:
        total_time = time.time() - start
        return {
            'total_time': total_time,
            'response': None,
            'error': execution_error,
            **action.to_dict()
        }
