from typing import Dict, List, Tuple, Union
from scipy.optimize import shgo
from .base_algorithm import BaseAlgorithm


class SHGOptimizer(BaseAlgorithm):

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int]]
    ) -> None:
        super().__init__(config)


    def optimize(self, func):
        return shgo(
            func,
            self.bounds,
            options={
                'maxfev': self.max_iter,
                'maxiter': self.max_iter,
                'local_iter': 1
            }
        )