from types import FunctionType


class Action:
    session=None
    timeout=0
    is_setup = False
    is_teardown = False
    before_batch: FunctionType = None
    after_batch: FunctionType = None
    
    def __init__(self) -> None:
        self.data = None

    async def setup(self):
        self.is_setup = True