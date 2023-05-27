class MonitorKilledError(Exception):
    
    def __init__(self, *args: object) -> None:
        super().__init__(
            'Process killed or aborted.'
        )