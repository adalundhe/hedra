from typing import Any, Dict, Callable
from scipy.optimize import OptimizeResult
from hedra.plugins.types.optimizer import (
    OptimizerPlugin,
    get,
    optimize,
    update
)


class CustomOptimizer(OptimizerPlugin):

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)

    @get()
    def get_params(self):
        return super().get_params()

    @update()
    def update_params(self):
        return super().update_params()

    @optimize()
    def run_dual_annealing(self, func: Callable[..., OptimizeResult]):
        pass