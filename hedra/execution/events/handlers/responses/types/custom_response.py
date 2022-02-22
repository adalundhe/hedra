from hedra.execution.events.handlers.responses import response


class CustomResponse:

    def __init__(self, action):
        self.name = action.get('name')
        self.env = action.get('env', 'N/A')
        self.type = action.get('type', 'N/A')
        self.time = action.get('total_time')
        self.user = action.get('user', 'N/A')
        self.url = action.get('url', 'N/A')
        self.tags = action.get('tags')
        self._response = action.get('response')
        self._error = action.get('error')
        self.success_condition = action.get('success_condition')
        self.status = None
        self.context = None

    async def assert_response(self):
        try:
            if self.success_condition:
                assert self.success_condition(self._response) == True

            if self._error:
                raise self._error

            self.status = 'SUCCESS'
            self.context = ''
        
        except Exception as error:
            self.status = 'FAILED'
            self.context = str(error)

    def to_dict(self):
        return {
            'event_name': self.name,
            'event_metric': self.time,
            'event_type': self.type,
            'event_status': self.status,
            'event_host': self.env,
            'event_url': self.url,
            'event_user': self.user,
            'event_tags': self.tags,
            'event_context': self.context
        }
