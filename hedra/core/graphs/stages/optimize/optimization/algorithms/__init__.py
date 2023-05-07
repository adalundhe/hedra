from typing import (
    List, 
    Tuple, 
    Union, 
    Dict, 
    Optional
)
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
    config: Dict[str, Union[List[Tuple[Union[int, float]]], int]],
    distribution_idx: Optional[int]=None
):
    return registered_algorithms.get(
        algorithm_type,
        SHGOptimizer
    )(
        config,
        distribution_idx=distribution_idx
    )