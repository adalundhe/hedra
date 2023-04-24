import math
import numpy
from typing import Union, List, Tuple, Optional
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
        self._lower_bound = 0.1
        self._upper_bound = 0.9

    def generate_distribution(self, batch_size: int) -> List[float]:
        return [
            math.ceil(
                step_batch_size
            ) for step_batch_size in self._generate_distribution(
                scale_factor=batch_size
            )
        ]
    
    def generate_non_scaled_distribution(self):
        return self._generate_distribution()
    
    def _generate_distribution(self, scale_factor: Optional[int]=None):
        distribution_size, step_size = self._get_distribution_and_step_size()

        distribution_sample = self._frozen_distribution.rvs(
            distribution_size
        ) * normal(
            scale=self._noise_scale,
            size=distribution_size
        )

        generated_walk = self._generate_random_walk(distribution_sample)

        smoothed_walk = self._smooth_generated_walk(
            generated_walk,
            step_size
        )

        if scale_factor:
            smoothed_walk *= scale_factor
        
        return self._apply_averaging(
            smoothed_walk,
            step_size
        )
    
    
    def _get_distribution_and_step_size(self) -> Tuple[int, int]:
        if self.size <= 10:
            distribution_size = self.size**3
            step_size = self.size**2

        elif self.size < 100:
            distribution_size = self.size**2
            step_size = self.size

        elif self.size < 1000:
            distribution_size = self.size * int(math.sqrt(self.size))
            step_size = int(math.sqrt(self.size))
            
        else:
            distribution_size = self.size * (1 + int(
                math.sqrt(self.size)/self.size
            ))

            step_size = (1 + int(
                math.sqrt(self.size)/self.size
            ))

        return distribution_size, step_size
    
    def _generate_random_walk(self, distribution_sample: List[float]) -> List[float]:
        current_step_value = 0.5
        result = []
        for sample_value in distribution_sample:

            if current_step_value < self._lower_bound:
                current_step_value = self._lower_bound

            elif current_step_value > self._upper_bound:
                current_step_value = self._upper_bound

            result.append(current_step_value)
            current_step_value += sample_value

        return numpy.array(result)
    
    def _smooth_generated_walk(
        self, 
        generated_walk: List[float], 
        smoothing_window_size: int
    ) -> List[float]:
        return numpy.convolve(
            generated_walk, 
            numpy.ones(
                (smoothing_window_size,)
            )/smoothing_window_size
        )[(smoothing_window_size-1):]
    
    def _apply_averaging(
        self, 
        scaled_walk: List[float],
        step_size: int
    ) -> List[float]:
        averaged_data = []

        for idx in range(0, len(scaled_walk), step_size):
            averaged_data.append(
                numpy.mean(scaled_walk[idx:idx + self.size])
            )

        return averaged_data