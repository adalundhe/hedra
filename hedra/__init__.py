import asyncio

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    uvloop.install()
    
except ImportError:
    pass