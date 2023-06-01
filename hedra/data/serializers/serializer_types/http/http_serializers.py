from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class HTTPSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        action: HTTPAction
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().action_to_serializable()
        return {
            **serialized_action,
            'type': RequestTypes.HTTP,
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
    ) -> HTTPAction:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})
        
        http_action = HTTPAction(
            name=action.get('name'),
            url=url_config.get('full'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', []),
            redirects=action.get('redirects', 3)
        )

        http_action.url.ip_addr = url_config.get('ip_addr')
        http_action.url.socket_config = url_config.get('socket_config')
        http_action.url.has_ip_addr = url_config.get('has_ip_addr')

        http_action.setup()

        return http_action
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryHTTPClient:
        return MercuryHTTPClient(
            concurrency=client_config.get('concurrency'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            ),
            reset_connections=client_config.get('reset_sessions')
        )