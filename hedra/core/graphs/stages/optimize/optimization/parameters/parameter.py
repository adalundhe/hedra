import math
from typing import Union
from .parameter_range import ParameterRange


class Parameter:

    def __init__(
            self,
            parameter_name: str,
            minimum: Union[int, float]=None,
            maximum: Union[int, float]=None,
            feed_forward: bool=False
        ) -> None:
        self.parameter_name = parameter_name
        self.feed_forward = feed_forward
        self.minimum = minimum
        self.maximum = maximum

        if minimum is None or minimum <= 0 and self.feed_forward:
            minimum = 0.5

        if maximum is None or maximum <= 0 and self.feed_forward:
            maximum = math.ceil(minimum) * 2

        self.range = ParameterRange(
            minimum_range=minimum,
            maximum_range=maximum
        )

        assert self.maximum > self.minimum, f"Err. - maximum parameter range value for optimization parameter {self.parameter_name} must be greater than minimum parameter range value."

        if self.feed_forward:
            assert self.minimum >= 0.1, f'Err. - Range multiplier for feed-forward optimization parameter {self.parameter_name} cannot exceed minimum of 0.1.'
            assert self.maximum <= 10, f'Err. - Range multiplier for feed-forward optimization parameter {self.parameter_name} cannot exceed maximum of 10.'
            