

class GraphQLResponse:

    def __init__(self, action) -> None:
        self.time = action.get('total_time')
        self.name = action.get('name')
        self.type = action.get('method')
        self.host = action.get('host')
        self.endpoint = action.get('endpoint')
        self.user = action.get('user')
        self.url = action.get('url')
        self.query = action.get('query')
        self.tags = action.get('tags')
        self._response = action.get('response')
        self._error = action.get('error')
        self.status = None
        self.context = None

    async def assert_response(self):

        if self._error:
            self.status = 'FAILED'
            self.context = str(self._error)

        elif hasattr(self._response, 'errors') and self._response.errors:
            error_messages = []
            for idx, error in enumerate(self._response.errors):
                error_message = error.get('message')
                error_string = f'Error: - {idx} - {error_message}'
                error_messages += [error_string]

            error_messages = '\n'.join(error_messages)
            self.status = 'FAILED'
            self.context = error_messages

        else:
            self.status = 'SUCCESS'
            self.context = self.query

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
