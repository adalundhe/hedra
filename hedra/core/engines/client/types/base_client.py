class BaseClient:
    initialized=False
    setup=False

    def __init__(self) -> None:
        self.initialized = True