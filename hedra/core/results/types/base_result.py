import traceback
from hedra.core.engines.types.common.response import Response


class BaseResult:

    def __init__(self, response: Response) -> None:
        self.name = response.name
        self.checks = response.checks
        self.error = response.error
        self.time = response.time
        self.type = response.type

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