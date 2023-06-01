from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class HTTP3Serializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def serialize_action(
        self,
        action: HTTP3Action
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().serialize_action()
        return {
            **serialized_action,
            'type': RequestTypes.HTTP3,
            'url': {
                'full': action.url.full,
                'ip_addr': action.url.ip_addr,
                'socket_config': action.url.socket_config,
                'has_ip_addr': action.url.has_ip_addr
            },
            'method': action.method,
            'headers': action._headers,
            'header_items': action._header_items,
            'encoded_headers': action.encoded_headers,
            'data': action.data,
            'encoded_data': action.encoded_data,
            'is_stream': action.is_stream,
            'redirects': action.redirects,
            'is_setup': action.is_setup,
            'action_args': action.action_args,
        }
    
    def deserialize_action(
        self,
        action: Dict[str, Any]
    ) -> HTTP3Action:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})
        
        http3_action = HTTP3Action(
            name=action.get('name'),
            url=url_config.get('full'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', [])
        )

        http3_action.url.ip_addr = url_config.get('ip_addr')
        http3_action.url.socket_config = url_config.get('socket_config')
        http3_action.url.has_ip_addr = url_config.get('has_ip_addr')

        http3_action.setup()

        return http3_action
    