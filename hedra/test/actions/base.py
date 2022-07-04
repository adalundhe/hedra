class Action:
    session=None
    timeout=0
    is_setup = False
    is_teardown = False
    
    def __init__(self) -> None:
        self.data = None

    async def setup(self):
        self.is_setup = True