class UpdateResponse:

    def __init__(self, update_response):
        self.field = update_response.get('event_name')
        self.message = update_response.get('message', 'OK')

    def to_dict(self):
        return {
            'field': self.field,
            'message': self.message
        }