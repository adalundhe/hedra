import uuid


class BaseClient:
    initialized=False
    setup=False

    def __init__(self) -> None:
        self.initialized = True
        self.metadata_string: str = None
        self.client_id = str(uuid.uuid4())