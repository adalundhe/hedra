from hedra.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hedra.reporting.metric.stage_metrics_summary import StageMetricsSummary
from hedra.reporting.system.system_metrics_set import SystemMetricsSet
from typing import Dict, Union


ExecutionResults = Dict[str, StageMetricsSummary]

GraphExecutionResults = Dict[str, Dict[str, Union[Dict[str, ExperimentMetricsSet], ExecutionResults, SystemMetricsSet]]]

SystemMetricsCollection = Dict[str, SystemMetricsSet]

GraphResults = Dict[str, Union[GraphExecutionResults, SystemMetricsCollection]]