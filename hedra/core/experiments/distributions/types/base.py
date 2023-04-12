from typing import Union
from numpy.random import normal

class SciPyDistribution:

    def rvs(self, size: int):
        return NotImplemented('Err. - This class is a generalized definition')


class BaseDistribution:

    def __init__(
        self, 
        size: Union[int,float]=1000,
        center: Union[int,float]=0.5,
        randomness: Union[int, float]=0.25,
        frozen_distribution: SciPyDistribution=None
    ) -> None:
        self.size = size
        self.center = center
        self.randomness = randomness
        self._frozen_distribution = frozen_distribution
        self._noise_scale = 0.01

    def generate(self):
        return self._frozen_distribution.rvs(
            self.size
        ) * normal(
            scale=self._noise_scale,
            size=self.size
        )