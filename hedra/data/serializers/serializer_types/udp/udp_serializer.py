from hedra.core.engines.types.udp.action import UDPAction
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class UDPSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        action: UDPAction
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().action_to_serializable()
        return {
            **serialized_action,
            'type': RequestTypes.UDP,
            'url': {
                'full': action.url.full,
                'ip_addr': action.url.ip_addr,
                'socket_config': action.url.socket_config,
                'has_ip_addr': action.url.has_ip_addr
            },
            'wait_for_response': action.wait_for_response,
            'data': action.data,
            'encoded_data': action.encoded_data,
            'is_stream': action.is_stream,
            'is_setup': action.is_setup,
            'action_args': action.action_args,
        }
    
    def deserialize_action(
        self,
        action: Dict[str, Any]
    ) -> UDPAction:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})
        
        udp_action = UDPAction(
            name=action.get('name'),
            url=url_config.get('full'),
            wait_for_response=action.get('wait_for_response', False),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', [])
        )

        udp_action.url.ip_addr = url_config.get('ip_addr')
        udp_action.url.socket_config = url_config.get('socket_config')
        udp_action.url.has_ip_addr = url_config.get('has_ip_addr')

        udp_action.setup()

        return udp_action
    