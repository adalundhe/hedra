from .types import (
    HttpAction,
    FastHttpAction,
    FastHttp2Action,
    CustomAction,
    PlaywrightAction,
    WebsocketAction,
    GrpcAction,
    GraphQLAction,
    MercuryHttpAction
)


class Action:

    action_types={
        'http': HttpAction,
        'fast-http': FastHttpAction,
        'fast-http2': FastHttp2Action,
        'action-set':CustomAction,
        'playwright': PlaywrightAction,
        'websocket': WebsocketAction,
        'grpc': GrpcAction,
        'graphql': GraphQLAction,
        'mercury-http': MercuryHttpAction
    }

    def __init__(self, action, type, group=None):
        self.type = self.action_types.get(
            type, 
            HttpAction
        )(
            action, 
            group=group
        )

    @classmethod
    def about(cls):
        
        action_types = '\n\t'.join([f'- {action_type}' for action_type in cls.action_types])

        return f'''
        Actions

        Actions are the base "unit" in Hedra - a single HTTP request, a single click or scroll command
        with Playwright. Borrowing a bit of terminology from physics, you can think of actions as
        units of "work". Hedra aims to maximize oth the amount of work done over the specified period of
        time and the amount of work done at any given point in time. The amount of work Hedra can successfuly
        process (both overall and at any given point in time) places strain on the system(s) Hedra is testing,
        generating "load" (an appropriate analogy would be stress) upon the targeted system.

        As each Engine in Hedra utilizes different underlying tooling and libraries, Hedra must pass different 
        types of actions. Hedra utilizes Parsers to abstract over the different requirements of action types, 
        providing a consistent interface for writing tests and a transparent means of interaction for both
        Personas (responsible for organizing how actions execute) and Engines (responsible for handling what
        actions execute).

        Hedra currently supports the following action types:

        {action_types}

        For more information on a given action type, run the command:

            hedra --about actions:<action_type>

        For more information on how actions are parsed, run the command:

            hedra --about actions:parsers


        Related Topics

        -personas
        -engines

        '''