from hedra.core.engines.types.common.context import Context
from hedra.test.client import Client
from .execute import Execute


class Warmup(Execute):
    name = None
    engine_type = 'http'
    context = Context()
    next_timeout = 0
    client: Client = None
    
    def __init__(self) -> None:
        super().__init__()