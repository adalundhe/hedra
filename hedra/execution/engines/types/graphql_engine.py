import asyncio
import uvloop
import logging
import time
from aiohttp.connector import TCPConnector
from aiohttp.resolver import AsyncResolver
from gql.transport.aiohttp import AIOHTTPTransport
from async_tools.datatypes.async_list import AsyncList
from .http_engine import HttpEngine
from .utils.wrap_awaitable import wrap_awaitable_future
from gql.transport.aiohttp import log as aiohttp_logger
aiohttp_logger.setLevel(logging.ERROR)


class GraphQLEngine(HttpEngine):

    def __init__(self, config, handler):
        super().__init__(config, handler)
        self.session_url = self.config.get('session_url')

    @classmethod
    def about(cls):
        return '''
        GraphQL Engine - (graphql)

        The GraphQL engine executes GraphQL queries against any valid GraphQL endpoint using
        the gql (v3.0) library.

        Actions are specified as:

        - endpoint: <graphql_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - method: <GET_or_POST>
        - headers: <headers_to_pass_to_graphql_session>
        - auth: <auth_to_pass_to_graphql_session>
        - query: <stringified_graphql_query_or_mutation_to_execute>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def yield_session(self):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop = asyncio.get_event_loop()

        connector = TCPConnector(
            limit=self._connection_pool_size, 
            ttl_dns_cache=self._dns_cache, 
            resolver=AsyncResolver(),
            keepalive_timeout=self._dns_cache,
            loop=loop
        )

        if self.request_timeout:
            transport = AIOHTTPTransport(
                url=self.session_url, 
                timeout=self.request_timeout, 
                client_session_args={
                    'connector': connector
                }
            )
        
        else:
            transport = AIOHTTPTransport(
                url=self.session_url, 
                client_session_args={
                    'connector': connector
                }
            )

        await transport.connect()
        return transport
