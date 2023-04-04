import numpy
import math
import random
from typing import Dict, Callable, List
from hedra.versioning.flags.types.unstable.flag import unstable
from .distribution_types import (
    DistributionTypes,
    DistributionMap
)


@unstable
class Distribution:

    def __init__(
            self,  
            distribution_type: DistributionTypes=DistributionTypes.NORMAL,
            intervals: int=None
        ) -> None:

        self._distribution_map = DistributionMap()
        self._distributions: Dict[str, Callable[..., List[float]]] = {
            DistributionTypes.BETA: lambda size: numpy.random.beta(1, 1, size=size),
            DistributionTypes.BINOMIAL: lambda size: numpy.random.binomial(
                size, 
                [random.uniform(0.1, 1) for _ in range(size)]
            )/100,
            DistributionTypes.CHI_SQUARE: lambda size: numpy.random.chisquare(
                size/2,
                size=size
            )/size,
            DistributionTypes.DIRICHLET: lambda size: [
                random.choice(
                    numpy.random.dirichlet([random.randint(1,10) for _ in range(3)])
                ) for _ in range(size)
            ],
            DistributionTypes.EXPONENITAL: lambda size: numpy.random.exponential(
                size/2,
                size
            )/size,
            DistributionTypes.F_DISTRIBUTION: lambda size: numpy.random.f(1, 100, size=size),
            DistributionTypes.GAMMA: lambda size: numpy.random.gamma(1, size=size),
            DistributionTypes.GEOMETRIC: lambda size: numpy.random.geometric(p=1/size, size=size)/size,
            DistributionTypes.GUMBEL: lambda size: numpy.random.gumbel(
                size/2,
                size/2,
                size=size
            )/size,
            DistributionTypes.HYPERGEOMETRIC: lambda size: numpy.random.hypergeometric(
                size * math.sqrt(size),
                size * math.sqrt(size),
                size,
                size=size
            )/size,
            DistributionTypes.LAPLACE: lambda size: numpy.random.laplace(
                size/2,
                math.sqrt(size)
            )/size,
            DistributionTypes.LOGISTIC: lambda size: numpy.random.laplace(
                size/2,
                math.sqrt(size),
                size=size
            )/size,
            DistributionTypes.MULTIVARIATE: lambda size: [
                random.choice(
                    numpy.random.multinomial(100, [0.1, 0.5, 1])/100
                ) for _ in range(size)
            ],
            DistributionTypes.NEGATIVE_BINOMIAL: lambda size: numpy.random.negative_binomial(
                size/2, 
                0.01, 
                size=size
            )/size,
            DistributionTypes.NONCENTRAL_CHISQUARE: lambda size: numpy.random.noncentral_chisquare(
                size/2, 
                math.sqrt(size), 
                size=size
            )/size,
            DistributionTypes.NONCENTRAL_F_DISTRIBUTION: lambda size: numpy.random.noncentral_f(
                1, 
                size, 
                size/2, 
                size=size
            )/size,
            DistributionTypes.NORMAL: lambda size: numpy.random.normal(
                size/2,
                math.sqrt(size), 
                size=size
            )/size,
            DistributionTypes.PARETO: lambda size: numpy.random.pareto(
                1, 
                size=size
            ),
            DistributionTypes.POISSON: lambda size: numpy.random.poisson(
                size/2, 
                size=size
            )/size,
            DistributionTypes.RAYLEIGH: lambda size: numpy.random.rayleigh(
                size/2, 
                size=size
            )/size,
            DistributionTypes.STANDARD_CAUCHY: lambda size: [
                val * -1 if val < 0 else val for val in numpy.random.standard_cauchy(size)
            ],
            DistributionTypes.STANDARD_EXPONENTIAL: lambda size: numpy.random.standard_gamma(
                1, 
                size=size
            ),
            DistributionTypes.STANDARD_NORMAL: lambda size: [
                val * -1 if val < 0 else val for val in numpy.random.standard_normal(size)
            ],
            DistributionTypes.STANDARD_T_DISTRIBUTION: lambda size: [
                val * -1 if val < 0 else val for val in numpy.random.standard_t(size, size=size)
            ],
            DistributionTypes.TRIANGULAR: lambda size: numpy.random.triangular(
                1, 
                size/2, 
                size, 
                size=size
            )/size,
            DistributionTypes.UNIFORM: lambda size: numpy.random.uniform(
                1,
                size, 
                size=size
            )/size,
            DistributionTypes.VONMISES: lambda size: numpy.random.vonmises(
                0.5, 
                size, 
                size=size
            ),
            DistributionTypes.WALD: lambda size: numpy.random.wald(
                0.5, 
                size,
                size=size
            ),
            DistributionTypes.WELBULL: lambda size: numpy.random.weibull(
                1,
                size=size
            ),
            DistributionTypes.ZIP_F_DISTRIBUTION: lambda size: numpy.random.zipf(
                random.uniform(1.1, 1.5), 
                size=size
            )/size
        }

        self.selected_distribution: DistributionTypes = self._distribution_map.get(
            distribution_type
        )
        self.disribution_function = self._distributions.get(
            self.selected_distribution
        )

        self.intervals = intervals

    def generate(self):
        distribution = self.disribution_function(self.intervals)
        for idx, val in enumerate(distribution):
            if val > 1:
                val = 1

            elif val < 0.1:
                val = 0.1

            distribution[idx] = round(val, 2)

        return distribution

    