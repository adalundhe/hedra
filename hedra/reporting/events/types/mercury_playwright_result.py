from mercury_http.playwright import Result
from .event_tags import EventTagCollection



class MercuryPlaywrightResult:

    def __init__(self, action: Result):
        self.time = action.time
        self.name = action.name
        self.type = action.method
        self.url = action.url
        self.user = action.user
        self.tags = EventTagCollection(action.tags)
        self._response = action
        self._error = action.error
        self.status = None
        self.url = action.url
        self.context = 'OK'
        
    async def assert_result(self):
        if self._error is not None:
            self.status = 'FAILURE'
            self.context = str(self._error)
        
        else:
            self.status = 'SUCCESS'
            
    def to_dict(self):
        return {
            'event_name': self.name,
            'event_metric': self.time,
            'event_type': self.type,
            'event_status': self.status,
            'event_host': None,
            'event_url': self.url,
            'event_user': self.user,
            'event_tags': self.tags.to_dict_list(),
            'event_context': self.context
        }