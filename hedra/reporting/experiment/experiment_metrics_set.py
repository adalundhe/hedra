from typing import Union, List

class ExperimentMetricsSet:

    def __init__(self) -> None:
        self.experiment_name: Union[str, None] = None
        self.randomized: Union[bool, None] = None
        self.participants: List[str] = []
        self.variants = {}
        self.mutations = []
        self.metrics = {}