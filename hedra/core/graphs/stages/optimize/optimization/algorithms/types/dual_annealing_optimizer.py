from typing import (
    Dict, 
    List, 
    Tuple, 
    Union, 
    Optional
)
from scipy.optimize import dual_annealing
from .base_algorithm import BaseAlgorithm


class DualAnnealingOptimizer(BaseAlgorithm):

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int]],
        distribution_idx: Optional[int]=None
    ) -> None:
        super().__init__(
            config,
            distribution_idx=distribution_idx
        )


    def optimize(self, func):
        return dual_annealing(
            func,
            self.bounds,
            maxiter=self.max_iter,
            maxfun=self.max_iter,
            no_local_search=True
        )