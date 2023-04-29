from .table_types import ExecutionResults


class ExecutionSummaryTable:

    def __init__(
        self,
        execution_results: ExecutionResults
    ) -> None:
        self.execution_results = execution_results
