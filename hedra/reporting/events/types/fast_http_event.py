from .http_event import HttpEvent


class FastHttpEvent(HttpEvent):

    def __init__(self, action):
        super(FastHttpEvent, self).__init__(action)

    async def assert_result(self):
        try:
            if self._response is None:
                raise self._error
            
            status_code = self._response.status_code

            if status_code < 200 or status_code > 299:
                context = await self._response.text()
                raise Exception(str(context))

            self.status = 'SUCCESS'
            
            self.context = ''
            if hasattr(self._response, 'reason'):
                self.context = self._response.reason
            
            self.url = f'{self.host}{self.endpoint}'

        except Exception as action_exception:
            self.status = 'FAILURE'
            self.url = '{host}{endpoint}'.format(host=self.host, endpoint=self.endpoint)
            self.context = str(action_exception)