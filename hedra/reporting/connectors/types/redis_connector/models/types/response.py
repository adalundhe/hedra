import json
import asyncio

class Response:

    def __init__(self, message, encoding):
        self.message = message
        self.encoding = encoding

    async def decode_pubsub_response(self) -> dict:

        message_channel = self.message.get(
            'channel'
        ).decode(self.encoding)

        message_pattern = self.message.get('pattern')
        if message_pattern:
            message_pattern = message_pattern.decode(self.encoding)

        message_data = self.message.get('data')
        if type(message_data) == bytes:
            message_data = await self._decode_data(message_data)

        return {
            'type': self.message.get('type'),
            'pattern': message_pattern,
            'channel': message_channel,
            'data': message_data
        }

    async def decode_pipeline_response(self) -> list:

        decoded_response = []

        for message_item in self.message:
            
            if type(message_item) == list:
                decoded_item = []
                for data in message_item:
                    decoded_data = await self._decode_data(data)
                    decoded_item.append(decoded_data)

                decoded_response.append(decoded_item)

            elif type(message_item) == dict:
                decoded_item = await self._decode_data(message_item)
                decoded_response.append(decoded_item)

            elif type(message_item) == set:
                decoded = await asyncio.gather(*[
                    self._decode_data(set_item) for set_item in message_item
                ])
                decoded_response.extend(decoded)

            elif type(message_item) == bytes:
                decoded_item = message_item.decode(self.encoding)
                decoded_response.append(decoded_item)

            else:
                decoded_response.append(message_item)

        return decoded_response


    async def _decode_data(self, data):
        try:
            data = json.loads(data)
        except Exception as JSONDecodeError:
            data = data.decode(self.encoding)

        return data
