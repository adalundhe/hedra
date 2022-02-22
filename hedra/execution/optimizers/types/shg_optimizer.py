from zebra_async_tools.functions import awaitable
from scipy.optimize import shgo


class SHGOptimizer:

    def __init__(self, bounds, max_iter=10) -> None:
        self.bounds = bounds
        self.max_iter = max_iter
        self.fixed_iters = False

    @classmethod
    def about(cls):
        return '''
        SHG Optimizer - (shg)

        The SHG Optimizer utilizes SciPy's Simple Homology Global Optimization (SHGO)
        algorithm. This algorithm is the default optimization algorithm and strikes
        a good balance between finding optimal parameters and number of iterations.
        '''

    async def optimize(self, func):
        return await awaitable(
            shgo,
            func,
            self.bounds,
            options={
                'maxfev': self.max_iter,
                'maxiter': self.max_iter,
                'local_iter': 0
            }
        )