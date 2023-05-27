from typing import Generic, TypeVar, List, Any, Dict
from hedra.core.hooks.types.condition.decorator import condition
from hedra.core.hooks.types.context.decorator import context
from hedra.core.hooks.types.event.decorator import event
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.monitoring import (
    CPUMonitor,
    MemoryMonitor
)
from hedra.plugins.types.plugin_types import PluginType
from hedra.reporting import Reporter
from hedra.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.system.system_metrics_group import SystemMetricsGroup
from hedra.reporting.system.system_metrics_set_types import SystemMetricGroupType
from hedra.reporting.system import SystemMetricsSet
from typing import Optional, Union


T = TypeVar('T')


class Submit(Stage, Generic[T]):
    stage_type=StageTypes.SUBMIT
    stream: bool = False
    config: T= None
    priority: Optional[str]=None
    retries: int=0
    
    def __init__(self) -> None:
        super().__init__()
        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.TRANSFORM
        ]

        self.source_internal_events = [
            'collect_process_results_and_metrics'
        ]

        self.internal_events = [
            'collect_process_results_and_metrics',
            'collect_reporter_plugins',
            'initialize_reporter',
            'check_for_events',
            'submit_processed_results',
            'submit_stage_metrics',
            'submit_main_metrics',
            'submit_error_metrics',
            'submit_custom_metrics',
            'complete_submit_session'
        ]

        self.stream = self.stream
        self.config = self.config
        self.priority = self.priority
        if self.priority is None:
            self.priority = 'auto'

        self.priority_level: StagePriority = StagePriority.map(
            self.priority
        )

        self.stage_retries = self.retries

    @Internal()
    async def run(self):

        await self.setup_events()
        self.dispatcher.assemble_execution_graph()
        await self.dispatcher.dispatch_events(self.name)

    @context()
    async def collect_process_results_and_metrics(
        self,
        submit_stage_session_total: int = 0,
        submit_stage_events: List[T]=[],
        submit_stage_summary_metrics: List[MetricsSet]=[],
        submit_stage_experiment_metrics: List[ExperimentMetricsSet]=[],
        submit_stage_streamed_metrics: Dict[str, StageStreamsSet] = {},
        submit_stage_system_metrics: List[SystemMetricsSet]=[],
    ):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Initializing results submission')

        self.context.ignore_serialization_filters = [
            'submit_stage_monitors',
            'session_stage_monitors'
        ]

        main_monitor_name = f'{self.name}.main'

        cpu_monitor = CPUMonitor()
        memory_monitor = MemoryMonitor()

        cpu_monitor.visibility_filters[main_monitor_name] = False
        memory_monitor.visibility_filters[main_monitor_name] = False

        cpu_monitor.stage_type = StageTypes.SUBMIT
        memory_monitor.stage_type = StageTypes.SUBMIT

        await cpu_monitor.start_background_monitor(main_monitor_name)
        await memory_monitor.start_background_monitor(main_monitor_name)

        return {
            'submit_stage_experiment_metrics': submit_stage_experiment_metrics,
            'submit_stage_session_total': submit_stage_session_total,
            'submit_stage_metrics': submit_stage_summary_metrics,
            'submit_stage_events': submit_stage_events,
            'submit_stage_streamed_metrics': submit_stage_streamed_metrics,
            'submit_stage_monitors': {
                'cpu': cpu_monitor,
                'memory': memory_monitor
            },
            'submit_stage_system_metrics': submit_stage_system_metrics
        }

    @event('collect_process_results_and_metrics')
    async def collect_reporter_plugins(self):

        reporter_plugins = self.plugins_by_type.get(PluginType.REPORTER)

        for plugin_name, plugin in reporter_plugins.items():
            Reporter.reporters[plugin_name] = plugin

            if isinstance(self.config, plugin.config):
                self.config.reporter_type = plugin_name
            
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Reporter plugin - {plugin_name}')

        return {
            'submit_stage_reporter_plugins': reporter_plugins
        }

    @event('collect_reporter_plugins')
    async def initialize_reporter(self):

        reporter = Reporter(self.config)
        reporter.graph_name = self.graph_name
        reporter.graph_id = self.graph_id
        reporter.stage_name = self.name
        reporter.stage_id = self.stage_id

        reporter_name = reporter.reporter_type_name

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Submitting results via - {reporter_name}:{reporter.reporter_id} - reporter')
        await self.logger.spinner.append_message(f'Submitting results via - {reporter_name} - reporter')

        await reporter.connect()


        return {
            'submit_stage_reporter': reporter,
            'submit_stage_reporter_name': reporter_name
        }
    
    @condition('initialize_reporter')
    async def check_for_streams(
        self,
        submit_stage_streamed_metrics: Dict[str, StageStreamsSet]={},
    ):
        return {
            'submit_stage_has_streams': len(submit_stage_streamed_metrics) > 0
        }
    
    @event('check_for_streams')
    async def submit_streamed_metrics(
        self,
        submit_stage_has_streams: bool=False,
        submit_stage_streamed_metrics: Dict[str, StageStreamsSet]={},
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None,
    ):
        if submit_stage_has_streams:
            submit_stage_streams_count = len(submit_stage_streamed_metrics)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting - {submit_stage_streams_count} - Streams')
            await submit_stage_reporter.submit_streams(submit_stage_streamed_metrics)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitted - {submit_stage_streams_count} - Streams')

    
    @condition('initialize_reporter')
    async def check_for_experiments(
        self,
        submit_stage_experiment_metrics: List[ExperimentMetricsSet]=[],

    ):
        return {
            'submit_stage_has_experiments': len(submit_stage_experiment_metrics) > 0
        }
    
    @event('check_for_experiments')
    async def submit_experiment_results(
        self,
        submit_stage_experiment_metrics: List[ExperimentMetricsSet]=[],
        submit_stage_has_experiments: bool = False,
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None,
    ):
        if submit_stage_has_experiments:
            submit_stage_experiments_count = len(submit_stage_experiment_metrics)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting - {submit_stage_experiments_count} - Experiments')
            await submit_stage_reporter.submit_experiments(submit_stage_experiment_metrics)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitted - {submit_stage_experiments_count} - Experiments')


    @condition('initialize_reporter')
    async def check_for_events(
        self,
        analyze_stage_events: List[Any]=[]
    ):
        return {
            'submit_stage_has_events': len(analyze_stage_events) > 0
        }

    @event('check_for_events')
    async def submit_processed_results(
        self,
        submit_stage_has_events: bool=False,
        submit_stage_events: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None,
        submit_stage_session_total: int=0
    ):

        if submit_stage_has_events and self.stream is False:

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting - {submit_stage_session_total} - Events')
            await submit_stage_reporter.submit_events(submit_stage_events)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitted - {submit_stage_session_total} - Events')

        return {}

    @event('initialize_reporter')
    async def submit_stage_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Common Metrics')
        await submit_stage_reporter.submit_common(submit_stage_metrics)

        return {}

    @event('initialize_reporter')
    async def submit_main_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await submit_stage_reporter.submit_metrics(submit_stage_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Metrics')

        return {}

    @event('initialize_reporter')
    async def submit_error_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await submit_stage_reporter.submit_errors(submit_stage_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Error Metrics')

        return {}

    @event('initialize_reporter')
    async def submit_custom_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await submit_stage_reporter.submit_custom(submit_stage_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Custom Metrics')

        return {}
    
    @event(
        'submit_experiment_results',
        'submit_processed_results',
        'submit_stage_metrics',
        'submit_main_metrics',
        'submit_error_metrics',
        'submit_custom_metrics'
    )
    async def close_stage_system_monitors(
        self,
        submit_stage_system_metrics: List[SystemMetricsSet]=[],
        submit_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]]={}
    ):
        stage_cpu_monitor: CPUMonitor = submit_stage_monitors.get('cpu')
        stage_memory_monitor: MemoryMonitor = submit_stage_monitors.get('memory')

        main_monitor_name = f'{self.name}.main'
    
        await stage_cpu_monitor.stop_background_monitor(main_monitor_name)
        await stage_memory_monitor.stop_background_monitor(main_monitor_name)

        stage_cpu_monitor.close()
        stage_memory_monitor.close()

        stage_cpu_monitor.stage_metrics[main_monitor_name] = stage_cpu_monitor.collected[main_monitor_name]
        stage_memory_monitor.stage_metrics[main_monitor_name] = stage_memory_monitor.collected[main_monitor_name]  

        submit_stage_monitors = {
            self.name: {
                'cpu': stage_cpu_monitor,
                'memory': stage_memory_monitor
            }
        }    

        system_metrics = SystemMetricsSet(
            submit_stage_monitors,
            {}
        )

        system_metrics.generate_system_summaries()

        for metrics_set in submit_stage_system_metrics:

            metrics_set.metrics.update(submit_stage_monitors)
            metrics_set.cpu_metrics_by_stage[self.name] = stage_cpu_monitor
            metrics_set.memory_metrics_by_stage[self.name] = stage_memory_monitor

            metrics_set.cpu = SystemMetricsGroup(
                metrics_set.cpu_metrics_by_stage,
                SystemMetricGroupType.CPU 
            )

            metrics_set.memory = SystemMetricsGroup(
                metrics_set.memory_metrics_by_stage,
                SystemMetricGroupType.MEMORY 
            )

            metrics_set.generate_system_summaries()

        return {
            'submit_stage_system_metrics': submit_stage_system_metrics,
            'stage_system_metrics': system_metrics
        }

    @event('close_stage_system_monitors')
    async def submit_system_metrics(
        self,
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None,
        submit_stage_system_metrics: List[SystemMetricsSet]=[]
    ):
        
        await submit_stage_reporter.submit_system_metrics(submit_stage_system_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting System Metrics')

        return {}

    @context('submit_system_metrics')
    async def complete_submit_session(
        self,
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None,
        submit_stage_session_total: int=0,
        submit_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]]={},
        stage_system_metrics: SystemMetricsSet=None
    ):

        await submit_stage_reporter.close()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Completed Metrics submission')
        await self.logger.spinner.set_default_message(f'Successfully submitted the results for {submit_stage_session_total} actions via {submit_stage_reporter_name} reporter')

        return {
            'submit_stage_monitors': submit_stage_monitors,
            'stage_system_metrics': stage_system_metrics
        }
    
    @event('complete_submit_session')
    async def close_session(self):
        return {}