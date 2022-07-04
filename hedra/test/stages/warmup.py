from .execute import (
    Execute,
    MercuryEngine
)



class Warmup(Execute):
    name=None
    engine_type='http'
    session: MercuryEngine =None
    config={}
    actions = []
    setup_actions = []
    teardown_actions = []
    
    def __init__(self) -> None:
        super().__init__()