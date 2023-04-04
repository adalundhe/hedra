from enum import Enum
from typing import Dict, Iterable
from hedra.versioning.flags.types.unstable.flag import unstable



class DistributionTypes(Enum):
    BETA='beta'
    BINOMIAL='binomial'
    CHI_SQUARE='chi-square'
    DIRICHLET='dirichlet'
    EXPONENITAL='exponential'
    F_DISTRIBUTION='f-dist'
    GAMMA='gamma'
    GEOMETRIC='geometric'
    GUMBEL='gumbel'
    HYPERGEOMETRIC='hypergeometric'
    LAPLACE='laplace'
    LOGISTIC='logistic'
    LOGNORMAL='lognormal'
    LOGSERIES='logseries'
    MULTIVARIATE='multivariate'
    MULTIVARIATE_NORMAL='multivariate-normal'
    NEGATIVE_BINOMIAL='negative-binomial'
    NONCENTRAL_CHISQUARE='noncentral-chisquare'
    NONCENTRAL_F_DISTRIBUTION='noncentral-f'
    NORMAL='normal'
    PARETO='pareto'
    POISSON='poisson'
    RAYLEIGH='rayleight'
    STANDARD_CAUCHY='standard-cauchy'
    STANDARD_EXPONENTIAL='standard-exponential'
    STANDARD_GAMMA='standard-gamma'
    STANDARD_NORMAL='standard-normal'
    STANDARD_T_DISTRIBUTION='standard-t'
    TRIANGULAR='triangular'
    UNIFORM='uniform'
    VONMISES='vonmises'
    WALD='wald'
    WELBULL='welbull'
    ZIP_F_DISTRIBUTION='zip-f'


@unstable
class DistributionMap:

    def __init__(self) -> None:
        self._types: Dict[str, DistributionTypes] = {
            distribution_type.value: distribution_type for distribution_type in DistributionTypes
        }

    def __iter__(self) -> Iterable[DistributionTypes]:
        for distribution_type in self._types.values():
            yield distribution_type

    def __getitem__(self, distribution_type: str) -> DistributionTypes:
        return self._types.get(
            distribution_type,
            DistributionTypes.NORMAL
        )

    def get(self, distribution_type: str) -> DistributionTypes:
        return self._types.get(
            distribution_type,
            DistributionTypes.NORMAL
        )