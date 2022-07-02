import json
import datetime
import time
import tzlocal
from mercury_http.common import Response
from .types import (
    MercuryPlaywrightResult,
    CustomEvent,
    MercuryHTTPEvent
)


class Event:
    event_types = {
        'playwright': MercuryPlaywrightResult,
        'custom': CustomEvent,
        'websocket': MercuryHTTPEvent,
        'grpc': MercuryHTTPEvent,
        'graphql': MercuryHTTPEvent,
        'http': MercuryHTTPEvent,
        'http2': MercuryHTTPEvent
    }

    def __init__(self, response: Response):
        self.data = self.event_types.get(response.type)(response)

        self.event_time = datetime.datetime.now()
        self.machine_timezone = tzlocal.get_localzone()
        self.machine_timezone = tzlocal.get_localzone()

    @classmethod
    def about(cls):

        event_types = '\n\t'.join([f'- {event_type}' for event_type in cls.event_types])

        return f'''
        Reporter Events


        Events are parsed action results to be submitted for storage and aggregation. The exact format of events
        varies slightly by the reporter type selected, but generally adheres to:

        - event name: Name of the action.

        - event metric: The value of the event to be used in aggregation calculation.

        - event type: The "type" of event - this varies by the action/engine type but 
                      examples include Playwright command, REST request method etc.

        - event status: Whether the action passed or failed.

        - event host: The base address of the target URI/URL the action executed against.

        - event url: The full address of the target URI/URL the action executed against.

        - event user: The user associated with the event (if any provided)

        - event tags: A list of dictionaries with `tag_name` and `tag_value` as keys. Note that
                      for some reporters, the name and value of a tag are combined as a single,
                      colon-delimited string (i.e. "<tag_name>:<tag_value>"). Tags are created
                      based upon the tags specified for a given action.

        - event context: Arbitrary additional information for the action, may be an array of
                         dictionaries, a string, etc. This is usually used to provide the
                         error message if an action failed or success reason if the action
                         succeeded.


        Events are linked to the update reporter type and generate timestamps upon submission to the resource specified
        by the update reporter. If an event is submitted that is not the correct type it will be automatically converted 
        to the type specified by the update reporter. Currently supported event types include:

        {event_types}
        
        For more information on exact event type specification for a given reporter, run the command:

            hedra --about results:events:<reporter_type>

        NOTE: As an end user, you will never need to directly interact with Events, their conversion, submission,
        or handling outside of specifying which update reporter Hedra should use.

        '''

    def get_naive_time(self):
        return datetime.datetime.utcnow()

    def get_utc_time(self):
        return self.event_time.astimezone(
            datetime.timezone.utc
        ).strftime(
            "%Y-%m-%dT%H:%M:%S%z"
        )

    def get_local_time(self):
        return self.machine_timezone.localize(
            self.event_time
        ).strftime(
            "%Y-%m-%dT%H:%M:%S:%z"
        )

    def get_posix_timestamp(self):
        created_time = datetime.datetime.strptime(
            self.get_local_time(),
            "%Y-%m-%dT%H:%M:%S:%z"
        )
        return time.mktime(
            created_time.timetuple()
        )
    
    def __str__(self):
        return json.dumps({
            'time_utc': self.get_utc_time(),
            'time_local': self.get_local_time(),
            **self.to_dict()
        })

    def __repr__(self):
        return json.dumps({
            'time_utc': self.get_utc_time(),
            'time_local': self.get_local_time(),
            **self.to_dict()
        })

    async def assert_response(self):
        await self.data.assert_result()

    def to_dict(self):
        return self.data.to_dict()