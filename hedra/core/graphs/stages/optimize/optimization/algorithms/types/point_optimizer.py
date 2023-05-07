from collections import defaultdict
from hedra.core.personas.batching.param_type import ParamType
from hedra.versioning.flags.types.unstable.flag import unstable
from numpy.random import normal
from statistics import mean
from typing import (
    Dict, 
    List, 
    Tuple, 
    Union, 
    Optional
)
from .base_algorithm import BaseAlgorithm


@unstable
class PointOptimizer(BaseAlgorithm):

    def __init__(
        self, 
        config: Dict[str, Union[List[Tuple[Union[int, float]]], int]],
        distribution_idx: Optional[int]=None
    ) -> None:
        super().__init__(
            config,
            distribution_idx=distribution_idx
        )

        self.params_history = defaultdict(list)
        self.next_params = {}
        self.error_history = defaultdict(list)
        self.stop_iteration = False
        self.error_batch_size_map = defaultdict(list)
        self.param_errors = defaultdict(lambda: defaultdict(list))
        self._noise_scale = 0.1
        self._target_value: int = None

    async def optimize(self, func):

        for param_name, boundary in self._boundaries_by_param_name.items():

            next_value = mean([boundary[0], boundary[1]])
            self.next_params[param_name] = next_value

            self.params_history[param_name].append(next_value)

        for param_name in self.optimize_params:
            for _ in range(self.max_iter):
                    error, target_value = await func(
                        list(self.next_params.values())
                    )

                    if error:
                        self._compute_next_params(
                            error,
                            target_value,
                            param_name
                        )

                    else:
                        boundary = self._boundaries_by_param_name.get(param_name)
                        next_value = mean([boundary[0], boundary[1]])
                        self.next_params[param_name] = next_value

                        self.params_history[param_name].append(next_value)

        final_params = {}
        minimized_error_mean = 0
        minimized_parameter = 0
        for param_name in self.optimize_params:

            param_errors = self.param_errors[param_name]
            param_error_sums = {}
            param_values_by_sum = defaultdict(list)

            for param_value, errors in param_errors.items():
                mean_errors = mean(errors)
                param_error_sums[param_value] = mean_errors
                param_values_by_sum[mean_errors].append(param_value)

            error_sums = list(param_error_sums.values())

            if len(error_sums) > 0:
                minimized_error_mean = min(error_sums)

            else:
                minimized_error_mean = 0
                minimized_parameter = self._target_value

            minimized_parameter = min(param_values_by_sum[minimized_error_mean])


            param = self.param_values.get(param_name, {})
            params_type = param.get('type')

            if params_type == ParamType.INTEGER:
                minimized_parameter = int(minimized_parameter)

            else:
                minimized_parameter = float(minimized_parameter)

            final_params[param_name] = {
                'minimized_distribution_value': minimized_parameter,
                'minimized_error_mean': minimized_error_mean
            }

        return final_params
            

    def _compute_next_params(
        self, 
        error: float, 
        target_value: Union[float, int],
        optimizing_param_name: str, 
    ):
        
        last_param_value = self.params_history[optimizing_param_name][-1]
        adjustment_ratio = 1
        absolute_error = abs(error)
        completed = error + target_value

        if self._target_value is None:
            self._target_value = target_value


        if error > 0:

            if target_value > absolute_error:
                adjustment_ratio = absolute_error/target_value

            else:
                adjustment_ratio = target_value/absolute_error

            noise_distribution = normal(
                scale=self._noise_scale,
                size=1
            )

            noise_value = noise_distribution[0]

            adjustment_ratio = abs(
                round(adjustment_ratio + noise_value, 2)
            )
            next_param_value = last_param_value * adjustment_ratio

        else:
            adjustment_ratio = absolute_error/target_value

            noise_distribution = normal(
                scale=self._noise_scale,
                size=1
            )

            noise_value = noise_distribution[0]            

            adjustment_ratio = abs(
                round(adjustment_ratio + noise_value, 2)
            )
            next_param_value = last_param_value * (1 + adjustment_ratio)
            
        minimum_boundary, maximum_boundary = self._boundaries_by_param_name[optimizing_param_name]

        if next_param_value < minimum_boundary:
            next_param_value = minimum_boundary

        elif next_param_value > maximum_boundary:
            next_param_value = maximum_boundary

        if next_param_value < 1:
            next_param_value = 1

        if completed > 0:
            self.next_params[optimizing_param_name] = next_param_value
            self.params_history[optimizing_param_name].append(next_param_value)
            self.error_history[optimizing_param_name].append(
                absolute_error
            )

            self.error_batch_size_map[absolute_error].append(last_param_value)
            self.param_errors[optimizing_param_name][last_param_value].append(absolute_error)


