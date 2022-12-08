from typing import Dict, List, Tuple, Union
from scipy.optimize import dual_annealing
from .base_algorithm import BaseAlgorithm


class DualAnnealingOptimizer(BaseAlgorithm):

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int]]
    ) -> None:
        super().__init__(config)


    def optimize(self, func):
        return dual_annealing(
            func,
            self.bounds,
            maxiter=self.max_iter,
            maxfun=self.max_iter,
            no_local_search=True
        )