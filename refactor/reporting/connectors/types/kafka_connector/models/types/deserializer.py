import json


class Deserializer:

    def __init__(self, config):
        self.deserializer_type = config.get('kafka_serializer_type')
        self.deserializer_encoding = config.get('kafka_serializer_encoding', 'utf-8')

        self.deserializer_types = {
            'json': self._json_deserializer,
            'string': self._string_deserializer,
            'default': self._default
        }

        self.selected_deserializer = self.deserializer_types.get(
            self.deserializer_type, 
            self._default
        )

    def _default(self, value) -> object:
        return value

    def _json_deserializer(self, value) -> dict:
        return json.loads(value.decode(self.deserializer_encoding))

    def _string_deserializer(self, value) -> object:
        return value.decode(self.deserializer_encoding)
