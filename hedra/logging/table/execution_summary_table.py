from collections import OrderedDict
from typing import List
from .table_types import ExecutionResults


class ExecutionSummaryTable:

    def __init__(
        self,
        execution_results: ExecutionResults
    ) -> None:
        
        self.execution_results = execution_results
        self.table_rows: List[OrderedDict] = []

    def _generate_stages_table(self):
        pass