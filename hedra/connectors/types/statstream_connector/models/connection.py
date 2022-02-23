import json
from statstream.streaming import StatStreamGroup
from .statement import Statement


class Connection:

    def __init__(self, config):
        self.config = config
        self.stream_config = config.get('stream_config', {})
        self.stream_name = self.stream_config.get('stream_name')
        self.connection = None
        self.results = []


    def connect(self):
        statement = Statement(self.stream_config)
        stream_config = statement.to_stream_config()
        self.connection = StatStreamGroup()
        self.connection.add_stream(stream_config)

    def execute(self, query) -> None:

        if query.get('stream_name') is None:
            query['stream_name'] = self.stream_name
            
        statement = Statement(query)

        if statement.type == 'update':
            event = statement.to_event()
            response = self.connection.update(event)

        elif statement.type == 'stat_query':
            query = statement.to_query()
            response = self.connection.get_field_stat(query)

        elif statement.type == 'field_query':
            query = statement.to_query()
            response = self.connection.get_field_stats(query)
        
        else:
            query = statement.to_query()
            response = self.connection.get_stream_stats(query)

        self.results.append(response)

    def commit(self) -> list:
        results = self.results
        self.results = []
        return results
        

    def clear(self) -> None:
        self.results = []

    def close(self):
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





