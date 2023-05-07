from typing import (
    Dict, 
    List, 
    Tuple, 
    Union, 
    Optional
)
from scipy.optimize import differential_evolution
from .base_algorithm import BaseAlgorithm


class DifferentialEvolutionOptimizer(BaseAlgorithm):

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
        return differential_evolution(
            func,
            self.bounds,
            maxiter=self.max_iter
        )
