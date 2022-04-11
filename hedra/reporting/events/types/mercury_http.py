from mercury_http.http.response import Response
from .event_tags import EventTagCollection


class MercuryHTTPEvent:

    def __init__(self, action: Response):
        self.time = action.time
        self.name = action.name
        self.type = action.method
        self.host = action.hostname
        self.endpoint = action.path
        self.user = action.user
        self.tags = EventTagCollection(action.tags)
        self._response = action
        self._error = action.error
        self.status = None
        self.url = action.url
        self.context = self._response.reason
        self._response_status = self._response.status
        self._response_reason = self._response.reason

    async def assert_result(self):
        try:
            if self._error is None:
                raise self._error
            
            if self._response.status < 200 or self._response.status >= 300:
                raise Exception(f'Request Failed - Status: {self._response_status}')

            self.status = 'SUCCESS'

        except Exception as action_exception:
            self.status = 'FAILURE'
            self.url = f'{self.host}{self.endpoint}'
            self.context = str(action_exception)
            
    def to_dict(self):
        return {
            'event_name': self.name,
            'event_metric': self.time,
            'event_type': self.type,
            'event_status': self.status,
            'event_host': self.host,
            'event_url': self.url,
            'event_user': self.user,
            'event_tags': self.tags.to_dict_list(),
            'event_context': self.context
        }