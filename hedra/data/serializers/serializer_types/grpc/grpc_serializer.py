from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class GRPCSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def serialize_action(
        self,
        action: GRPCAction
    ) -> Dict[str, Union[str, List[str]]]:
        serialized_action = super().serialize_action(
            action
        )

        return {
            **serialized_action,
            'type': RequestTypes.GRPC,
            'url': {
                'full': action.url.full,
                'ip_addr': action.url.ip_addr,
                'socket_config': action.url.socket_config,
                'has_ip_addr': action.url.has_ip_addr
            },
            'method': action.method,
            'headers': action._headers,
            'data': action.data,
            'is_stream': action.is_stream,
            'is_setup': action.is_setup,
            'action_args': action.action_args,
        }
    
    def deserialize_action(
        self,
        action: Dict[str, Any]
    ) -> GRPCAction:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})

        grpc_action = GRPCAction(
            name=action.get('name'),
            url=url_config.get('full'),
            method=action.get('method'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', [])
        )

        grpc_action.url.ip_addr = url_config.get('ip_addr')
        grpc_action.url.socket_config = url_config.get('socket_config')
        grpc_action.url.has_ip_addr = url_config.get('has_ip_addr')

        grpc_action.setup()

        return grpc_action