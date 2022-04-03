import uuid
from async_tools.datatypes.async_list import AsyncList
from easy_logger import Logger
from hedra.reporting.events import Event
from hedra.command_line import CommandLine
from hedra.reporting.connectors.types.statstream_connector import StatStreamConnector as StatsStream


class ParallelHandler:

    def __init__(self, config: CommandLine):
        self.config = config
        self.runner_mode = self.config.runner_mode

        self.session_id = str(uuid.uuid4())

        self.connector = StatsStream({
            'stream_config': {
                'stream_name': self.session_id
            }
        })

        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')

    async def aggregate(self, actions):

        event_names = set()
        await self.connector.connect()

        async for action in AsyncList(actions):
            event = Event(action)
            await event.assert_response()

            # response_dict = response.to_dict()
            # event = Event(
            #     event_type='statstream',
            #     data=response_dict
            # )
    
            event_names.add(event.data.name)

            await self.connector.execute({
                'type': 'update',
                'stream_name': self.session_id,
                **event.to_dict()
            })

        await self.connector.commit()
        
        for event_name in event_names:
            await self.connector.execute({
                'stream_name': self.session_id,
                'key': event_name,
                'type': 'field_query'
            })
                
        return await self.connector.commit()

            
        
        