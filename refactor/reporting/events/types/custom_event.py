import traceback
from .event_tags import EventTagCollection


class CustomEvent:

    def __init__(self, action):
        self.name = action.get('name')
        self.env = action.get('env', 'N/A')
        self.type = action.get('type', 'N/A')
        self.time = action.get('total_time')
        self.user = action.get('user', 'N/A')
        self.url = action.get('url', 'N/A')
        self.tags = EventTagCollection(action.get('tags', []))
        self._response = action.get('response')
        self._error = action.get('error')
        self.checks = action.get('checks', [])
        self.status = None
        self.context = None

    async def assert_result(self):
        try:
            for check in self.checks:
                await check(self._response, self.time)

            if self._error:
                raise self._error

            self.status = 'SUCCESS'
            self.context = ''
        
        except Exception as error:
            self.status = 'FAILED'

            traceback_stack = traceback.format_exc().splitlines()
            error_message = f'{traceback_stack[-1].strip()} - {traceback_stack[-2].strip()}'
            if len(str(error)) > 1:
                error_message = str(error)

            self.context = error_message

    def to_dict(self):
        return {
            'event_name': self.name,
            'event_metric': self.time,
            'event_type': self.type,
            'event_status': self.status,
            'event_host': self.env,
            'event_url': self.url,
            'event_user': self.user,
            'event_tags': self.tags.to_dict_list(),
            'event_context': self.context
        }
