from mercury_http.http import MercuryHTTPClient
from .http_action import HttpAction

class MercuryHttpAction(HttpAction):

    def __init__(self, action, group=None):
        super(MercuryHttpAction, self).__init__(action, group)
        self.ssl = action.get('ssl', False)
        self.action_type = 'mercury-http'

    @classmethod
    def about(cls):
        return '''
        Mercury-HTTP Action

        Mercury-HTTP Actions represent a single HTTP 1.X/2.X REST call using Hedra's Mercury-HTTP
        or Mercury-HTTP2 engine. For example - a GET request to https://www.google.com/.

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
        - ssl: <boolean_to_use_ssl> (defaults to False)
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''
    
    def execute(self, session: MercuryHTTPClient):
        return session.request(
            self.name,
            self.url,
            method=self.method,
            headers=self.headers,
            params=self.params,
            data=self.data,
            ssl=self.ssl
        )