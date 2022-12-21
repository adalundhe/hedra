from typing import List, Tuple, Union, Dict
from .types import (
    SHGOptimizer,
    DualAnnealingOptimizer,
    DifferentialEvolutionOptimizer
)


registered_algorithms = {
        'shg': SHGOptimizer,
        'dual-annealing': DualAnnealingOptimizer,
        'diff-evolution': DifferentialEvolutionOptimizer
}


def get_algorithm(
    algorithm_type: str,
    config: Dict[str, Union[List[Tuple[Union[int, float]]], int]]
):
    return registered_algorithms.get(
        algorithm_type,
        SHGOptimizer
    )(config)