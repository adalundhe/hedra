import time
import logging
import functools
import os
import asyncio
import grpc


def connect_with_retry(wait_buffer=5,timeout_threshold=int(os.getenv('GPRC_CONNECT_TIMEOUT', 30))): 
    def retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start = time.time()
            elapsed = 0
            result = None
            while elapsed < timeout_threshold:
                try:
                    result = func(*args, **kwargs)
                    break
                except Exception as error:
                    result = None
                    logging.error('Error: Connection attempt failed. Retrying in {wait_buffer}'.format(wait_buffer=wait_buffer))
                    logging.debug(str(error))
                if result:
                    return result

                time.sleep(wait_buffer)
                current = time.time()
                elapsed = current - start

            return result
        return wrapper
    return retry


def connect_with_retry_async(wait_buffer=5,timeout_threshold=int(os.getenv('GPRC_CONNECT_TIMEOUT', 30))): 
    def retry(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            elapsed = 0
            result = None
            while elapsed < timeout_threshold:
                try:
                    result = await func(*args, **kwargs)
                    break
                except Exception as error:
                    result = None
                    logging.error('Error: Connection attempt failed. Retrying in {wait_buffer}'.format(wait_buffer=wait_buffer))
                    logging.debug(str(error))
                if result:
                    return result

                await asyncio.sleep(wait_buffer)
                current = time.time()
                elapsed = current - start

            return result
        return wrapper
    return retry



def connect_or_return_none(wait_buffer=5,timeout_threshold=int(os.getenv('GPRC_CONNECT_TIMEOUT', 30))): 
    def retry(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start = time.time()
            elapsed = 0
            result = None
            while elapsed < timeout_threshold:
                try:
                    result = await func(*args, **kwargs)

                    if isinstance(result, grpc.aio.AioRpcError):
                        result = None

                    break
                except Exception as error:
                    result = None
                if result:
                    return result

                await asyncio.sleep(wait_buffer)
                current = time.time()
                elapsed = current - start

            return result
        return wrapper
    return retry