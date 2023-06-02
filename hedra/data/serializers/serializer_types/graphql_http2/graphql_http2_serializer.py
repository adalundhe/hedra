from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.graphql_http2.client import MercuryGraphQLHTTP2Client
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class GraphQLHTTP2Serializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        action: GraphQLHTTP2Action
    ) -> Dict[str, Union[str, List[str]]]:
        serialized_action = super().action_to_serializable(action)

        return {
            **serialized_action,
            'type': RequestTypes.GRAPHQL_HTTP2,
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
    ) -> GraphQLHTTP2Action:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})

        graphql_http2_action = GraphQLHTTP2Action(
            name=action.get('name'),
            url=url_config.get('full'),
            method=action.get('method'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', [])
        )

        graphql_http2_action.url.ip_addr = url_config.get('ip_addr')
        graphql_http2_action.url.socket_config = url_config.get('socket_config')
        graphql_http2_action.url.has_ip_addr = url_config.get('has_ip_addr')

        graphql_http2_action.setup()

        return graphql_http2_action
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryGraphQLHTTP2Client:
        return MercuryGraphQLHTTP2Client(
            concurrency=client_config.get('concurrency'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            ),
            reset_connections=client_config.get('reset_sessions')
        )