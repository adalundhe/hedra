
from codecs import StreamReader
from typing import Awaitable
from typing import Tuple
from hedra.core.engines.types.common.response import Response
from hedra.core.engines.types.common.constants import NEW_LINE


async def read_body(reader: StreamReader, response: Response) -> Awaitable[Response]:
        while True:
            chunk = await reader.readline()
            if chunk == b'0\r\n' or chunk is None:
                response.body += b'\r\n'
                break
            response.body += chunk

        return response


async def http_headers_to_iterator(reader: StreamReader) -> Awaitable[Tuple[str, str]]:
        """Transform loop to iterator."""
        while True:
            # StreamReader already buffers data reading so it is efficient.
            res_data = await reader.readline()
            if b": " not in res_data and b":" not in res_data:
                break
            
            decoded = res_data.rstrip().decode()
            pair = decoded.split(": ", 1)
            if pair and len(pair) < 2:
                pair = decoded.split(":")

            key, value = pair
            yield key.lower(), value