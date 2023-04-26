from hedra.core.experiments.variant import Variant
from scipy.optimize import least_squares
from typing import Dict, List, Tuple, Union
from .base_algorithm import BaseAlgorithm


class LeastSquaresOptimizer(BaseAlgorithm):

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int]]
    ) -> None:
        super().__init__(config)
        self.initial_distribution: List[float] = []

    def generate_initial_distribution(self, variant: Variant):
        self.initial_distribution = variant.distribution.generate(self.stage_config.batch_size)

    def optimize(self, func):
        return least_squares(
            func,
            self.initial_distribution,
            diff_step=[
                0.5 for _ in self.distribution
            ],
            max_nfev=self.max_iter
        )