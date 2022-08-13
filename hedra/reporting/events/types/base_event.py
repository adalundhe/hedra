import traceback
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.reporting.tags import Tag


class BaseEvent:

    def __init__(self, result: BaseResult) -> None:
        self.name = None
        self.shortname = result.name
        self.error = result.error
        self.time = result.read_end - result.start
        self.time_waiting = result.start - result.wait_start
        self.time_connecting = result.connect_end - result.start
        self.time_writing = result.write_end - result.connect_end
        self.time_reading = result.read_end - result.write_end
        self.type = result.type
        self.source = result.source
        self.type = result.type

        self.checks = []

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