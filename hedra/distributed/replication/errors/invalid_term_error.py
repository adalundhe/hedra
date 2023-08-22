class InvalidTermError(Exception):

    def __init__(
        self,
        entry_id: int,
        entry_term: int,
        expected_term: int
    ) -> None:
        super().__init__(
            f'Log entry - {entry_id} - provided invalid term - {entry_term} - Expected term - {expected_term}'
        )