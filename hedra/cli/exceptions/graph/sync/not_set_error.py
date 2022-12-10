class NotSetError(Exception):

    def __init__(self, path: str, missing_item: str, flag: str) -> None:
        super().__init__(
            f'\n\nError - {missing_item} found for graph repostiory at {path}.\nSet the initial value for {missing_item} by providing the {flag} option once or check your path.\n'
        )