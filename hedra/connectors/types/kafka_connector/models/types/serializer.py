import json


class Serializer:

    def __init__(self, config):
        self.serializer_type = config.get('kafka_serializer_type')
        self.serializer_encoding = config.get('kafka_serializer_encoding', 'utf-8')

        self.serializer_types = {
            'json': self._json_serializer,
            'string': self._string_serializer,
            'default': self._default
        }

        self.selected_serializer = self.serializer_types.get(
            self.serializer_type, 
            self._default
        )

    def _default(self, value) -> object:
        return value

    def _json_serializer(self, value) -> bytes:
        return json.dumps(value).encode(self.serializer_encoding)

    def _string_serializer(self, value) -> bytes:
        return str(value).encode(self.serializer_encoding)
