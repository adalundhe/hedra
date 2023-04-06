class ProcessKilledError(Exception):
    
    def __init__(self, *args: object) -> None:
        super().__init__(
            'Process killed or aborted.'
        )