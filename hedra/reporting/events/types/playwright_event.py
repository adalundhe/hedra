from .event_tags import EventTagCollection


class PlaywrightEvent:

    def __init__(self, action):
        
        self.name = action.get('name')
        self.context = {
            'selector': action.get('selector'),
            'data': action.get('data'),
            'options': action.get('options')
        }
        self.time = action.get('total_time')
        self.type = action.get('type')
        self.host = action.get('env')
        self.url = action.get('url')
        self.user = action.get('user')
        self.tags = EventTagCollection(action.get('tags', []))
        self._response = action.get('response')
        self._error = action.get('error')
        self.status = None

    async def assert_response(self):
        try:
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
            'event_host': self.host,
            'event_url': self.url,
            'event_user': self.user,
            'event_tags': self.tags.to_dict_list(),
            'event_context': self.context
        }
