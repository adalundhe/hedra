from typing import (
    Dict, 
    List, 
    Tuple, 
    Union, 
    Optional
)
from scipy.optimize import shgo
from .base_algorithm import BaseAlgorithm


class SHGOptimizer(BaseAlgorithm):

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
        return shgo(
            func,
            self.bounds,
            options={
                'maxfev': self.max_iter,
                'maxiter': self.max_iter
            }
        )