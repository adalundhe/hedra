import binascii
from typing import Dict, Union
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http2.result import HTTP2Result
from .action import GRPCAction


class GRPCResult(HTTP2Result):

    def __init__(self, action: GRPCAction, error: Exception = None) -> None: 
        super(GRPCResult, self).__init__(action, error)
        self.type = RequestTypes.GRPC

    @property
    def data(self):
        wire_msg = binascii.b2a_hex(self.body)

        message_length = wire_msg[4:10]
        msg = wire_msg[10:10+int(message_length, 16)*2]

        return binascii.a2b_hex(msg)

    @data.setter
    def data(self, value):
        self.body = value

    def to_protobuf(self, protobuf):
        protobuf.ParseFromString(self.data)
        return protobuf

    @classmethod
    def from_dict(cls, results_dict: Dict[str, Union[int, float, str,]]):
        
        action = GRPCAction(
            results_dict.get('name'),
            results_dict.get('url'),
            method=results_dict.get('method'),
            user=results_dict.get('user'),
            tags=results_dict.get('tags'),
        )
        
        data = results_dict.get('data')
        if data:
            encoded_protobuf = str(binascii.b2a_hex(data), encoding='raw_unicode_escape')
            encoded_message_length = hex(int(len(encoded_protobuf)/2)).lstrip("0x").zfill(8)
            encoded_protobuf = f'00{encoded_message_length}{encoded_protobuf}'

            data = binascii.a2b_hex(encoded_protobuf)

        response = GRPCResult(action, error=results_dict.get('error'))
        

        response.headers.update(results_dict.get('headers', {}))
        response.data = data
        response.status = results_dict.get('status')
        response.reason = results_dict.get('reason')
        response.checks = results_dict.get('checks')
     
        response.wait_start = results_dict.get('wait_start')
        response.start = results_dict.get('start')
        response.connect_end = results_dict.get('connect_end')
        response.write_end = results_dict.get('write_end')
        response.complete = results_dict.get('complete')

        return response