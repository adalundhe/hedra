from enum import Enum
from typing import Dict, Iterable
from hedra.versioning.flags.types.unstable.flag import unstable



class DistributionTypes(Enum):
    ALPHA='alpha'
    ANGLIT='anglit'
    ARCSINE='arcsine'
    ARGUS='argus'
    BETA_PRIME='beta-prime'
    BETA='beta'
    BRADFORD='bradford'
    BURR12='burr12'
    BURR='burr'
    CAUCHY='cauchy'
    CHI_SQUARED='chi-squared'
    CHI='chi'
    COSINE='cosine'
    CRYSTAL_BALL='crystal-ball'
    DGAMMA='dgamma'
    DWEIBULL='dweibull'
    ERLANG='erlang'
    EXPONENTIAL_NORMAL='exponential-normal'
    EXPONENTIAL_POWER='exponential-power'
    EXPONENTIAL='exponential'
    F_DISTRIBUTION='f-distribution'
    FATIGUE_LIFE='fatigue-life'
    FISK='fisk'
    FOLDED_CAUCHY='folded-cauchy'
    FOLDED_NORMAL='folded-normal'
    GAMMA='gamma'
    GAUSS_HYPERGEOMETRIC='gauss-hypergeometric'
    GENERALIZED_EXPONENTIAL='generalized-exponential'
    GENERALIZED_EXTREME='generalized-extreme'
    GENERALIZED_GAMMA='generalized-gamma'
    GENERALIZED_HALF_LOGISTIC='generalized-half-logistic'
    GENERALIZED_HYPERBOLIC='generalized-hyperbolic'
    GENERALIZED_LOGISTIC='generalized-logistic'
    GENERALIZED_NORMAL='generalized-normal'
    GENERALIZED_PARETO='generalized-pareto'
    GENERALIZED_INVERSE_GAUSS='generalized-inverse-gauss'
    GIBRAT='gibrat'
    GOMPERTZ='gompertz'
    GUMBEL_L='gumbel-l'
    GUMBEL_R='gumbel-r'
    HALF_CAUCHY='half-cauchy'
    HALF_GENERALIZED_NORMAL='half-generalized-normal'
    HALF_LOGISTIC='half-logistic'
    HALF_NORMAL='half-normal'
    HYPERBOLIC_SECANT='hyperbolic-secant'
    INVERSE_GAMMA='inverse-gamma'
    INVERSE_WEIBULL='inverse-weibull'
    JOHNSON_SB='johnson-db'
    JOHNSON_SU='johnson-su'
    KAPPA_3='kappa-3'
    KAPPA_4='kappa-4'
    KS_ONE='ks-one'
    KS_TWO_BIGENERALIZED='ks-two-bigeneralized'
    KS_TWO='ks-two'
    LAPLACE_ASYMMETRIC='laplace-asymmetric'
    LAPLACE='laplace'
    LEVY_L='levy-l'
    LEVY_STABLE='levy-stable'
    LEVY='levy'
    LOG_GAMMA='log-gamma'
    LOG_LAPLAPCE='log-laplace'
    LOG_UNIFORM='log-uniform'
    LOGISTIC='logistic'
    LOMAX='lomax'
    MAXWELL='maxwell'
    MIELKE='mielke'
    MOYAL='moyal'
    NAKAGAMI='nakagami'
    NON_CENTRAL_F_DISTRIBUTION='non-centeral-f'
    NON_CENTRAL_CHI_SQUARED='non-central-chisqr'
    NON_CENTRAL_T_DISTRIBUTION='non-central-t'
    NORMAL_INVERSE_GAUSS='normal=inverse-gauss'
    NORMAL='normal'
    PARETO='pareto'
    PEARSON_3='pearson-3'
    POWER_LOG_NORMAL='power-log-normal'
    POWER_NORMAL='power-normal'
    POWERLAW='powerlaw'
    R_DISTRIBUTION='r-distribution'
    RAYLEIGH='rayleigh'
    RECIPROCAL_INVERSE_GAUSS='reciprocal-inverse-gauss'
    RICE='rice'
    SEMI_CIRCULAR='semi-circular'
    SKEWED_CAUCHY='skewed-cauchy'
    SKEWED_NORMAL='skewed-normal'
    STUDENT_RANGE='student-range'
    T_DISTRIBUTION='t-distribution'
    TRAPEZOID='trapezoid'
    TRIANGULAR='triangular'
    TRUNCATED_EXPONENTIAL='truncated-exponential'
    TRUNCATED_NORMAL='truncated-normal'
    TRUNCATED_PARETO='truncated-pareto'
    TRUNCATED_WEIBULL_MINIMUM='truncated-weibull-minimum'
    TUKEY_LAMBDA='tuke-lambda'
    UNIFORM='uniform'
    VONMISES_LINE='vonmises-line'
    VONMISES='vonmises'
    WALD='wald'
    WEIBULL_MAXIMUM='weibull-maximum'
    WEIBULL_MINIMUM='weibull-minimum'
    WRAPPED_CAUCHY='wrapped-cauchy'


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