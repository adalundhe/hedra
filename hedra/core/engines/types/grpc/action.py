import binascii
from typing import Dict, Iterator, List, Union
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http2.action import HTTP2Action


class GRPCAction(HTTP2Action):

    def __init__(
        self,
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        data: Union[str, dict, Iterator, bytes, None] = None, 
        user: str=None, 
        tags: List[Dict[str, str]] = []
    ) -> None:
    
        super(
            GRPCAction, 
            self
        ).__init__(
            name, 
            url, 
            method, 
            headers, 
            data, user, 
            tags
        )

        self.timeout = 60
        self.type = RequestTypes.GRPC
        self.hooks: Hooks[GRPCAction] = Hooks()

    def _setup_headers(self):
        grpc_headers = {
            'Content-Type': 'application/grpc',
            'Grpc-Timeout': f'{self.timeout}S',
            'TE': 'trailers'
        }
        self._headers.update(grpc_headers)

        super(
            GRPCAction,
            self
        )._setup_headers()

    def _setup_data(self) -> None:
        encoded_protobuf = str(binascii.b2a_hex(self.data.SerializeToString()), encoding='raw_unicode_escape')
        encoded_message_length = hex(int(len(encoded_protobuf)/2)).lstrip("0x").zfill(8)
        encoded_protobuf = f'00{encoded_message_length}{encoded_protobuf}'

        self.encoded_data = binascii.a2b_hex(encoded_protobuf)