from easy_logger import Logger


class HttpResponse:

    def __init__(self, action):
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.time = action.get('total_time')
        self.name = action.get('name')
        self.type = action.get('method')
        self.host = action.get('host')
        self.endpoint = action.get('endpoint')
        self.user = action.get('user')
        self.tags = action.get('tags')
        self._response = action.get('response')
        self._error = action.get('error')
        self.status = None
        self.url = None
        self.context = None

    async def assert_response(self):
        try:
            if self._response is None:
                raise self._error
            
            self._response.raise_for_status()
            self.status = 'SUCCESS'
            
            self.context = ''
            if hasattr(self._response, 'reason'):
                self.context = self._response.reason
            
            self.url = str(self._response.url)

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
            'event_tags': self.tags,
            'event_context': self.context
        }