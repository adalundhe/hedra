import traceback
import uuid
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.reporting.tags import Tag


class BaseEvent:

    __slots__ = (
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

    def __init__(self, result: BaseResult) -> None:

        self.event_id = str(uuid.uuid4())
        self.action_id = result.action_id

        self.name = None
        self.shortname = result.name
        self.error = result.error
        self.timings = {}
        self.type = result.type
        self.source = result.source
        self.checks = []

        self.time = self.timings.get('total', 0)

        for check_hook_name in result.checks:
            check_hook = registrar.all.get(check_hook_name)
            self.checks.append(check_hook.call)

        self.tags = [
            Tag(
                tag.get('name'),
                tag.get('value')
            ) for tag in result.tags
        ]
        self.stage = None

    async def check_result(self):
        if self.error is None:
            for check in self.checks:
                try:
                    await check(self)
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