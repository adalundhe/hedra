from .http_action import HttpAction

class FastHttpAction(HttpAction):

    def __init__(self, action, group=None):
        super(FastHttpAction, self).__init__(action, group=group)
        self.action_type = 'fast-http'

    @classmethod
    def about(cls):
        return '''
        Fast-HTTP Action

        Fast-HTTP Actions represent a single HTTP 1.X/2.X REST call using Hedra's Fast-HTTP
        or Fast-HTTP2 engine. For example - a GET request to https://www.google.com/.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''
    
    async def execute(self, session):
        return await session.request(
            self.url,
            self.method,
            headers=self.headers,
            params=self.params,
            data=self.data
        )