from .event_tags import EventTagCollection


class GrpcEvent:

    def __init__(self, action) -> None:
        self.name = action.get('name')
        self.host = action.get('host')
        self.rpc_name = action.get('endpoint')
        self.data = action.get('data')
        self.streaming_type = action.get('streaming_type')
        self.order = action.get('order')
        self.weight = action.get('weight')
        self.type = action.get('method')
        self.user = action.get('user')
        self.tags = EventTagCollection(action.get('tags', []))
        self.response = action.get('response')
        self.url = f'{self.host}.{self.rpc_name}'
        self._error = action.get('error')
        self.time = action.get('total_time')
        self.status = None
        self.context = None

    async def assert_response(self):

        if self._error:
            self.status = 'FAILED'
            self.context = self._error.details()

        else:
            self.status = 'SUCCESS'
            self.context = ''
            
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