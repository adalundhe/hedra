
import io
import base64
import struct
from hedra.distributed.discovery.dns.core.exceptions import (
    DNSError
)
from hedra.distributed.discovery.dns.core.record import (
    QueryType,
    Record,
    RecordType
)
from pydantic import (
    StrictInt, 
    StrictBool
)
from typing import (
    List, 
    Tuple,
    Union,
    Iterable,
    Dict,
    Optional
)
from hedra.distributed.models.http import (
    HTTPRequest, 
    HTTPRequestMethod
)
from hedra.distributed.models.base.message import Message




class DNSMessage(Message):
    query_type: QueryType=QueryType.REQUEST
    query_id: StrictInt=0
    query_opcode: StrictInt=0
    query_authoritative_answer: StrictInt=0
    query_truncation: StrictInt=0
    query_desired_recursion: StrictInt=0
    query_available_recursion: StrictInt=0
    query_result_code: StrictInt=0
    record_types: List[RecordType]=[]
    query_domains: List[Record]=[]
    query_answers: List[Record]=[]
    query_namservers: List[Record]=[]
    query_additional_records: List[Record]=[]
    query_has_result: StrictBool=False

    class Config:
        arbitrary_types_allowed=True


    def __iter__(self):
        return iter(self.query_answers)
    
    def is_request(self):
        return self.query_type
    
    @classmethod
    def get_bits(
        cls,
        num: int, 
        bit_len: int
    ):

        high = num >> bit_len
        low = num - (high << bit_len)

        return low, high
    
    @staticmethod
    def parse_entry(
        query_type: QueryType, 
        data: bytes, 
        cursor_posiition: int,
        length: int
    ) -> Tuple[int, List[Record]]:
        
        results: List[Record] = []

        for _ in range(length):
            record = Record(query_type.value)
            cursor_posiition = record.parse(data, cursor_posiition)

            results.append(record)

        return cursor_posiition, results

    @classmethod
    def parse(
        cls, 
        data: bytes, 
        query_id: Optional[bytes] = None
    ):

        (
            request_id, 
            raw_data, 
            domains, 
            answers, 
            nameservers, 
            additional_records
        ) = struct.unpack('!HHHHHH', data[:12])

        if query_id is not None and query_id != request_id:
            raise DNSError(-1, 'Transaction ID mismatch')
        
        result_code, raw_data = cls.get_bits(raw_data, 4)  # rcode: 0 for no error

        _, raw_data = cls.get_bits(raw_data, 3)  # reserved

        available_recursion, raw_data = cls.get_bits(raw_data, 1)  # recursion available

        desired_recursion, raw_data = cls.get_bits(raw_data, 1)  # recursion desired

        truncation, raw_data = cls.get_bits(raw_data, 1)  # truncation

        authoritative_answer, raw_data = cls.get_bits(raw_data, 1)  # authoritative answer

        opcode, raw_data = cls.get_bits(raw_data, 4)  # opcode
        
        query_type, raw_data = cls.get_bits(raw_data, 1)  # qr: 0 for query and 1 for response

        cursor_position, query_domains = cls.parse_entry(
            QueryType.REQUEST.value, 
            data, 
            12, 
            domains
        )

        cursor_position, query_answers = cls.parse_entry(
            QueryType.RESPONSE.value, 
            data, 
            cursor_position, 
            answers
        )

        cursor_position, query_nameservers = cls.parse_entry(
            QueryType.RESPONSE.value, 
            data, 
            cursor_position, 
            nameservers
        )

        _, query_additional_records = cls.parse_entry(
            QueryType.RESPONSE.value, 
            data, 
            cursor_position, 
            additional_records
        )

        return DNSMessage(
            query_type=QueryType.by_value(query_type),
            query_opcode=opcode,
            query_authoritative_answer=authoritative_answer,
            query_truncation=truncation,
            query_desired_recursion=desired_recursion,
            query_available_recursion=available_recursion,
            query_result_code=result_code,
            query_domains=query_domains,
            query_answers=query_answers,
            query_namservers=query_nameservers,
            query_additional_records=query_additional_records
        )

    def get_record(
        self, 
        record_types: Union[RecordType, Iterable[RecordType]]
    ):
        '''Get the first record of qtype defined in `qtypes` in answer list.
        '''
        if isinstance(record_types, RecordType):
            record_types = record_types

        for item in self.query_answers:
            if item.record_types in record_types:
                return item.data
            
    def pack(
        self, 
        size_limit: int = None
    ) -> bytes:


        names: Dict[str, int] = {}
        buffer = io.BytesIO()
        buffer.seek(12)
        truncation = 0

        query_groups = [
            self.query_domains, 
            self.query_answers, 
            self.query_namservers, 
            self.query_additional_records
        ]

        for group in query_groups:
            if truncation: 
                break

            for record in group:
                offset = buffer.tell()
                packed_record = record.pack(names, offset)

                if size_limit is not None and offset + len(packed_record) > size_limit:
                    truncation = 1
                    break

                buffer.write(packed_record)

        self.query_truncation = truncation
        buffer.seek(0)

        query_type = self.query_type.value << 15
        query_opcode = self.query_opcode << 11
        query_authoritative_answer = self.query_authoritative_answer << 10
        query_truncation = truncation << 9
        query_desired_recursion = self.query_desired_recursion << 8
        query_available_recursion = self.query_available_recursion << 7
        query_buffer_extra = 0 << 4
        query_result_code = self.query_result_code

        query_data = sum([
            query_type,
            query_opcode,
            query_authoritative_answer,
            query_truncation,
            query_desired_recursion,
            query_available_recursion,
            query_buffer_extra,
            query_result_code
        ])



        buffer.write(
            struct.pack(
                '!HHHHHH', 
                self.query_id, 
                query_data,
                len(self.query_domains),
                len(self.query_answers), 
                len(self.query_namservers), 
                len(self.query_additional_records)
            )
        )

        return buffer.getvalue()
    
    def to_http_bytes(
        self,
        url: str,
        method: HTTPRequestMethod=HTTPRequestMethod.GET
    ) -> bytes:
        
        message = self.pack()
        params: Dict[str, str] = {}
        data: Union[str, None] = None

        if method == HTTPRequestMethod.GET:
            params['dns'] = base64.urlsafe_b64encode(message).decode().rstrip('=')

        else:
            data = message.decode()
            
        http_request = HTTPRequest(
            host=self.host,
            port=self.port,
            error=self.error,
            url=url,
            method=method,
            headers={
                'accept': 'application/dns-message',
                'content-type': 'application/dns-message',
            },
            data=data
        )

        return http_request.prepare_request()
    
    def to_tcp_bytes(self) -> Tuple[bytes, bytes]:
        message = self.pack()
        message_size = len(message)

        return struct.pack('!H', message_size), + message
    
    def to_udp_bytes(self) -> bytes:
        return self.pack()
