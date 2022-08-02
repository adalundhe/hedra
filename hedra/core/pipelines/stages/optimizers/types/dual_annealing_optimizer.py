from scipy.optimize import dual_annealing
from async_tools.functions.awaitable import awaitable


class DualAnnealingOptimizer:

    def __init__(self, bounds, max_iter=10) -> None:
        self.bounds = bounds
        self.max_iter = max_iter
        self.fixed_iters = True
        self.iters = 0
    
    @classmethod
    def about(cls):
        return '''
        Dual Annealing - (dual-annealing)

        The Dual Annealing Optimizer uses SciPy's dual-annealing optimizer,
        albeit with local search disabled (by default) to ensure the algorithm 
        converges quickly and only executes the specified number of iterations. 
        The algorithm is good and finding optimal params over smaller search 
        spaces.

        '''

    async def optimize(self, func):
        return await awaitable(
                dual_annealing,
                func,
                self.bounds,
                maxiter=self.max_iter,
                maxfun=self.max_iter,
                no_local_search=True
            )