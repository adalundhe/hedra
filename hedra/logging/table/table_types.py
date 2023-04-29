from hedra.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hedra.reporting.metric.stage_metrics_summary import StageMetricsSummary
from typing import Dict, Union


ExecutionResults = Dict[str, StageMetricsSummary]

GraphExecutionResults = Dict[str, Dict[str, Union[Dict[str, ExperimentMetricsSet], ExecutionResults]]]

