import binascii
import json
from codecs import StreamWriter
from ctypes import Union
from typing import Dict, Iterator, Union
from urllib.parse import urlencode
from graphql import Source, parse, print_ast
from async_tools.datatypes import AsyncList
from .constants import NEW_LINE


class Payload:

    def __init__(self, payload: Union[str, dict, Iterator, bytes, None]) -> None:
        self.data = payload
        self.is_stream = False
        self.size = 0
        self.has_data = payload is not None
        self.encoded_data = b''

    async def __aiter__(self):
        yield self.data

    def __iter__(self):
        for chunk in self.data:
            yield chunk
    
    def setup_payload(self, no_chunking=False) -> None:
        if self.data and no_chunking is False:
            if isinstance(self.data, Iterator):
                chunks = []
                for chunk in self.data:
                    chunk_size = hex(len(chunk)).replace("0x", "") + NEW_LINE
                    encoded_chunk = chunk_size.encode() + chunk + NEW_LINE.encode()
                    self.size += len(encoded_chunk)
                    chunks.append(encoded_chunk)

                self.is_stream = True
                self.encoded_data = AsyncList(chunks)

            else:

                if isinstance(self.data, (Dict, tuple)):
                    self.encoded_data = urlencode(self.data)

                if isinstance(self.data, str):
                    self.encoded_data = self.data.encode()

                self.size = len(self.data)

    def setup_graphql_query(self) -> None:
        source = Source(self.data.get("query"))
        document_node = parse(source)
        query_string = print_ast(document_node)
        
        self.size = len(query_string)
        
        query = {
            "query": query_string
        }

        operation_name = self.data.get("operation_name")
        variables = self.data.get("variables")
        
        if operation_name:
            query["operationName"] = operation_name
        
        if variables:
            query["variables"] = variables

        self.encoded_data = json.dumps(query).encode()

    def setup_grpc_protobuf(self) -> None:
        encoded_protobuf = str(binascii.b2a_hex(self.data.SerializeToString()), encoding='raw_unicode_escape')
        encoded_message_length = hex(int(len(encoded_protobuf)/2)).lstrip("0x").zfill(8)
        encoded_protobuf = f'00{encoded_message_length}{encoded_protobuf}'

        self.encoded_data = binascii.a2b_hex(encoded_protobuf)

    async def write_chunks(self, writer: StreamWriter):
        async for chunk in self.data:
            writer.write(chunk)

        writer.write(("0" + NEW_LINE * 2).encode())
        

