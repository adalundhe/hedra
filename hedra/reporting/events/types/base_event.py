import traceback
from hedra.core.engines.types.common.response import Response
from hedra.reporting.tags import Tag


class BaseEvent:

    def __init__(self, response: Response) -> None:
        self.name = None
        self.shortname = response.name
        self.checks = response.checks
        self.error = response.error
        self.time = response.time
        self.type = response.type
        self.source = response.hostname

        self.tags = [
            Tag(
                tag.get('name'),
                tag.get('value')
            ) for tag in response.tags
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