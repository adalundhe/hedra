import traceback
import asyncio
import uuid
import inspect
from typing import Any, Coroutine, List, Dict, Any
from hedra.core.graphs.hooks.types.base.event import BaseEvent
from hedra.core.graphs.hooks.types.base.event_types import EventType
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.hooks.types.base.registrar import registrar
from hedra.reporting.tags import Tag


class BaseProcessedResult:

    __slots__ = (
        'stage_name',
        'event_id',
        'action_id',
        'name',
        'shortname',
        'error',
        'type',
        'source',
        'checks',
        'tags',
        'stage',
        'timings',
        'time'
    )

    def __init__(
        self, 
        stage: Any, 
        result: BaseResult
    ) -> None:

        self.event_id = str(uuid.uuid4())
        self.action_id = result.action_id

        self.name = None
        self.shortname = result.name
        self.error = result.error
        self.timings = {}
        self.type = result.type
        self.source = result.source
        self.checks: List[List[BaseEvent]] = []
        self.stage_name = stage.name

        self.time = self.timings.get('total', 0)

        action_or_task_event = stage.dispatcher.actions_and_tasks.get(result.name)

        if action_or_task_event:
            self.checks = [
                [
                    stage.dispatcher.events_by_name.get(check_name) for check_name in layer
                ] for layer in result.checks
            ]

        else:
            self.checks = []

        self.tags = [
            Tag(
                tag.get('name'),
                tag.get('value')
            ) for tag in result.tags
        ]
        self.stage = None

    async def check_result(self, result: Any):
        next_args = {
            'result': result
        }
        for layer in self.checks:
            results = await asyncio.gather(*[
                asyncio.create_task(
                    self._run_check(event, next_args)
                ) for event in layer
            ])

            for result in results:
                next_args.update(result)

    async def _run_check(self, check: BaseEvent, hook_args: Dict[str, Any]) -> Dict[str, Any]:

        try:
            result = await check.call(**hook_args)
            if isinstance(result, dict) is False:
                result = {
                    check.event_name: result
                }

        except AssertionError:              
            error_message = traceback.format_exc().split(
                '\n'
            )[-3].strip()

            self.error = f'Check - {error_message} - for action - {self.name} - failed.'
            result = {
                check.event_name: Exception(f'Check - {error_message} - for action - {self.name} - failed.')
            }

        return result


    @property
    def fields(self):
        return ['name', 'stage', 'time', 'succeeded']

    @property
    def values(self):
        return [self.name, self.stage, self.time, self.success]

    @property
    def record(self):
        return {
            'name': self.name,
            'stage': self.stage,
            'time': self.time,
            'succeeded': self.success
        }

    @property
    def success(self):
        return self.error is None

    def tags_to_dict(self):
        return {
            tag.name: tag.value for tag in self.tags
        }