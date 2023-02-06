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
        events: List[T]=[],
        summaries: Dict[str, Any]={}

    ):

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Initializing results submission')

        session_total = summaries.get('session_total', 0)
        metrics = []

        stage_summaries = summaries.get('stages', {})
        for stage in stage_summaries.values():
            metrics.extend(list(
                stage.get('actions', {}).values()
            ))

        return {
            'session_total': session_total,
            'metrics': metrics,
            'events': events
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
            'reporter_plugins': reporter_plugins
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
            'reporter': reporter,
            'reporter_name': reporter_name
        }

    @condition('initialize_reporter')
    async def check_for_events(
        self,
        events: List[Any]=[]
    ):
        return {
            'has_events_to_process': len(events) > 0
        }

    @event('check_for_events')
    async def submit_processed_results(
        self,
        has_events_to_process: bool=False,
        events: List[Any]=[],
        reporter: Reporter=None,
        reporter_name: str=None,
        session_total: int=0
    ):

        if has_events_to_process:

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Submitting - {session_total} - Events')
            await reporter.submit_events(events)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Submitted - {session_total} - Events')

    @event('initialize_reporter')
    async def submit_stage_metrics(
        self,
        metrics: List[Any]=[],
        reporter: Reporter=None,
        reporter_name: str=None
    ):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Submitting Common Metrics')
        await reporter.submit_common(metrics)

    @event('initialize_reporter')
    async def submit_main_metrics(
        self,
        metrics: List[Any]=[],
        reporter: Reporter=None,
        reporter_name: str=None
    ):
        await reporter.submit_metrics(metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Submitting Metrics')

    @event('initialize_reporter')
    async def submit_error_metrics(
        self,
        metrics: List[Any]=[],
        reporter: Reporter=None,
        reporter_name: str=None
    ):
        await reporter.submit_errors(metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Submitting Error Metrics')

    @event('initialize_reporter')
    async def submit_custom_metrics(
        self,
        metrics: List[Any]=[],
        reporter: Reporter=None,
        reporter_name: str=None
    ):
        await reporter.submit_custom(metrics)
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Submitting Custom Metrics')

    @event(
        'submit_processed_results',
        'submit_stage_metrics',
        'submit_main_metrics',
        'submit_error_metrics',
        'submit_custom_metrics'
    )
    async def complete_submit_session(
        self,
        reporter: Reporter=None,
        reporter_name: str=None,
        session_total: int=0
    ):

        await reporter.close()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Reporter - {reporter_name}:{reporter.reporter_id} - Completed Metrics submission')
        await self.logger.spinner.set_default_message(f'Successfully submitted the results for {session_total} actions via {reporter_name} reporter')
        