from hedra.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hedra.reporting.metric.metrics_set import MetricsSet
from typing import Dict, Union

StageActionsResults = Dict[str, MetricsSet]
ExecutionResults = Dict[str, Dict[str, Union[int, float,StageActionsResults]]]


GraphExecutionResults = Dict[str, Dict[str, Union[Dict[str, ExperimentMetricsSet], ExecutionResults]]]

