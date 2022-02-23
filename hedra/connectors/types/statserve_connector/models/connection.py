
import json
from statserve import StatServeClient
from .statement import Statement


class Connection:

    def __init__(self, config):
        self.config = config
        self.stream_config = config.get('stream_config', {})
        self.connection = StatServeClient(self.config)
        self.results = []

    async def connect(self) -> None:
        await self.connection.register(self.stream_config)

    async def execute(self, query) -> None:
        
        statement = Statement(query)

        if statement.type == 'update':
            event = await statement.to_event()
            self.results += [await self.connection.update(event.to_dict())]

        elif statement.type == 'stat_query':
            stat_query = await statement.to_query()
            response = await self.connection.get_field_stat(stat_query.to_dict())
            response_statement = Statement(response)
            metrics_summary = await response_statement.to_field_stats_summary()
            self.results.extend(metrics_summary.to_dict_list())

        elif statement.type == 'field_query':
            stats_query = await statement.to_query()
            response = await self.connection.get_field_stats(stats_query.to_dict())

            response_statement = Statement(response)
            metrics_summary = await response_statement.to_field_stats_summary()
            self.results.extend(metrics_summary.to_dict_list())

        else:
            stats_query = await statement.to_query()
            response = await self.connection.get_stream_stats(stats_query.to_dict())

            response_statement = Statement(response)
            metrics_summary = await response_statement.to_stream_summary()
            self.results.extend(metrics_summary.to_dict_list())

    async def execute_stream(self, queries):
        events = []

        for query in queries:
            statement = Statement(query)
            event = await statement.to_event()
            events.append(event.to_dict())

        self.results += [result async for result in self.connection.stream_update(events)]

    async def commit(self) -> list:
        results = self.results
        self.results = []
        return results
        

    async def clear(self) -> None:
        self.results = []

    async def close(self):
        if self.config.get('save_to_file'):

            output_filepath = self.config.get('output_filepath', 'results.json')
            output_results = []

            for result in self.results:

                if isinstance(result, dict):
                    output_results.append(result)

                else:
                    output_results.append(result.to_dict())

            if len(output_results) > 0:
                with open(output_filepath, self.config.get('output_file_mode', 'w')) as output_file:
                    json.dump(
                        output_results, 
                        output_file, 
                        indent=self.config.get('output_indent', 4)
                    )




