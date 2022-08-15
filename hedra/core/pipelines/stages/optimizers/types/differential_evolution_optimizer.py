from scipy.optimize import differential_evolution
from async_tools.functions import awaitable


class DifferentialEvolutionOptimizer:

    def __init__(self, bounds, max_iter=10) -> None:
        self.bounds = bounds
        self.max_iter = max_iter
        self.fixed_iters = False
        self.iters = 0

    @classmethod
    def about(cls):
        return '''
        Differential Evolution - (diff-evolution)

        The Differential Evolution Optimizer uses SciPy's differential evolution
        algorithm. Ideal for searching over larger spaces, but may require a significant
        amount of iterations to find optimal parameters.
        '''

    def optimize(self, func):
        return differential_evolution(
            func,
            self.bounds,
            maxiter=self.max_iter
        )
