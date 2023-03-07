class TransitionMetadata:

    def __init__(
        self, 
        allow_multiple_edges: bool,
        is_valid: bool
    ) -> None:
        self.allow_multiple_edges = allow_multiple_edges
        self.is_valid = is_valid