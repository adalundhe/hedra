from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.udp.action import UDPAction
from hedra.core.engines.types.udp.client import MercuryUDPClient
from hedra.core.engines.types.udp.result import UDPResult
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
        
        serialized_action = super().action_to_serializable(action)
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
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryUDPClient:
        return MercuryUDPClient(
            concurrency=client_config.get('concurrency'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            ),
            reset_connections=client_config.get('reset_sessions')
        )
    
    def result_to_serializable(
        self,
        result: UDPResult
    ) -> Dict[str, Any]:
        
        serialized_result = super().result_to_serializable(result)

        body: Union[str, None] = None
        if result.body:
            body = str(result.body.decode())

        return {
            **serialized_result,
            'url': result.url,
            'path': result.path,
            'params': result.params,
            'query': result.query,
            'type': result.type,
            'body': body,
            'tags': result.tags,
            'user': result.user,
            'status': result.status,
            'error': str(result.error)
        }
    
    def deserialize_result(
        self,
        result: Dict[str, Any]
    ) -> UDPResult:
        
        deserialized_result = UDPResult(
            UDPAction(
                name=result.get('name'),
                url=result.get('url'),
                method=result.get('method'),
                headers=result.get('headers'),
                data=result.get('data'),
                user=result.get('user'),
                tags=result.get('tags', [])      
            ),
            error=Exception(result.get('error'))
        )

        body = result.get('body')
        if isinstance(body, str):
            body = body.encode()

        deserialized_result.body = body
        deserialized_result.status = result.get('status')
        deserialized_result.checks = result.get('checks')
        deserialized_result.wait_start = result.get('wait_start')
        deserialized_result.start = result.get('start')
        deserialized_result.connect_end = result.get('connect_end')
        deserialized_result.write_end = result.get('write_end')
        deserialized_result.complete = result.get('complete')

        deserialized_result.type = RequestTypes.UDP

        return deserialized_result