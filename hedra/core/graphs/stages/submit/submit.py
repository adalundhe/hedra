from typing import Generic, TypeVar, List, Any, Dict
from hedra.core.graphs.hooks.hook_types.condition import condition
from hedra.core.graphs.hooks.hook_types.context import context
from hedra.core.graphs.hooks.hook_types.event import event
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook,
    ContextHook,
    TransformHook
)
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.plugins.types.plugin_types import PluginType
from hedra.reporting import Reporter
from hedra.core.graphs.stages.base.stage import Stage


T = TypeVar('T')


class Submit(Stage, Generic[T]):
    stage_type=StageTypes.SUBMIT
    config: T= None
    
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

    @Internal()
    async def run(self):

        await self.setup_events()
        await self.dispatcher.dispatch_events()

    @context()
    async def collect_process_results_and_metrics(
        self,
        analyze_stage_events: List[T]=[],
        analyze_stage_summaries: Dict[str, Any]={}

    ):

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Initializing results submission')

        session_total = analyze_stage_summaries.get('session_total', 0)
        metrics = []

        stage_summaries = analyze_stage_summaries.get('stages', {})
        for stage in stage_summaries.values():
            metrics.extend(list(
                stage.get('actions', {}).values()
            ))

        return {
            'submit_stage_session_total': session_total,
            'submit_stage_metrics': metrics,
            'submit_stage_events': analyze_stage_events
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

        if submit_stage_has_events:

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting - {submit_stage_session_total} - Events')
            await submit_stage_reporter.submit_events(submit_stage_events)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitted - {submit_stage_session_total} - Events')

    @event('initialize_reporter')
    async def submit_stage_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Common Metrics')
        await submit_stage_reporter.submit_common(submit_stage_metrics)

    @event('initialize_reporter')
    async def submit_main_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await submit_stage_reporter.submit_metrics(submit_stage_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Metrics')

    @event('initialize_reporter')
    async def submit_error_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await submit_stage_reporter.submit_errors(submit_stage_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Error Metrics')

    @event('initialize_reporter')
    async def submit_custom_metrics(
        self,
        submit_stage_metrics: List[Any]=[],
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None
    ):
        await submit_stage_reporter.submit_custom(submit_stage_metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Submitting Custom Metrics')

    @event(
        'submit_processed_results',
        'submit_stage_metrics',
        'submit_main_metrics',
        'submit_error_metrics',
        'submit_custom_metrics'
    )
    async def complete_submit_session(
        self,
        submit_stage_reporter: Reporter=None,
        submit_stage_reporter_name: str=None,
        submit_stage_session_total: int=0
    ):

        await submit_stage_reporter.close()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {submit_stage_reporter_name}:{submit_stage_reporter.reporter_id} - Completed Metrics submission')
        await self.logger.spinner.set_default_message(f'Successfully submitted the results for {submit_stage_session_total} actions via {submit_stage_reporter_name} reporter')
        