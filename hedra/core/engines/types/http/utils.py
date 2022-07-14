from typing import Awaitable
from typing import Tuple
from hedra.core.engines.types.common.response import Response
from .connection import Connection


async def read_body(connection: Connection, response: Response) -> Awaitable[Response]:
        while True:
            chunk = await connection.readline()
            if chunk == b'0\r\n' or chunk is None:
                response.body += b'\r\n'
                break
            response.body += chunk

        return response


async def http_headers_to_iterator(connection: Connection) -> Awaitable[Tuple[str, str]]:
        """Transform loop to iterator."""
        while True:
            # StreamReader already buffers data reading so it is efficient.
            res_data = await connection.readline()
            if b": " not in res_data and b":" not in res_data:
                break
            
            decoded = res_data.rstrip().decode()
            pair = decoded.split(": ", 1)
            if pair and len(pair) < 2:
                pair = decoded.split(":")

            key, value = pair
            yield key.lower(), value