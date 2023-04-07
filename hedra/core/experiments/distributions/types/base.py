from typing import Union


class SciPyDistribution:

    def rvs(self, size: int):
        return NotImplemented('Err. - This class is a generalized definition')


class BaseDistribution:

    def __init__(
        self, 
        size: Union[int,float],
        center: Union[int,float],
        scale: Union[int, float],
        frozen_distribution: SciPyDistribution
    ) -> None:
        self.size = size
        self.center = center
        self.scale = scale
        self._frozen_distribution = frozen_distribution

    def generate(self):
        return self._frozen_distribution.rvs(self.size)