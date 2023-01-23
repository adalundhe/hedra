import traceback
import asyncio
import uuid
import inspect
from typing import Any, Coroutine, Tuple, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.reporting.tags import Tag


class BaseEvent:

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
        self.checks = []
        self.stage_name = stage.name

        self.time = self.timings.get('total', 0)
        self.checks = [
            check for check in stage.hooks[HookType.CHECK] if check.name in result.checks
        ]

        self.tags = [
            Tag(
                tag.get('name'),
                tag.get('value')
            ) for tag in result.tags
        ]
        self.stage = None

    async def check_result(self):
        if self.error is None:
            await asyncio.gather(*[
                self._check_results(check) for check in self.checks
            ])

    async def _check_results(self, check: Coroutine):
        try:
            await check.call(self)
        except AssertionError:              
            error_message = traceback.format_exc().split(
                '\n'
            )[-3].strip()

            self.error = f'Check - {error_message} - for action - {self.name} - failed.'

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