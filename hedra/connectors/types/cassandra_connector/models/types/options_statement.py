from .utils import (
    format_values,
    format_value
)

class OptionsStatment:

    def __init__(self, keyspace, table_name, options):
        self.keyspace = keyspace
        self.table_name = table_name
        self.options = options
        self._option_types = {
            'timestamp': self.append_time_statement,
            'if_not_exists': self.append_if_not_exists,
            'filter': self.append_filter_statement,
            'sort': self.append_sort_statement,
            'limit': self.append_limit_statement
        }
        self.query = None

    async def assemble_options(self, query) -> str:
        self.query = query

        for option in self.options:
            option_type = option.get('type')
            option_statement = self._option_types.get('option_type')

            if option_statement:
               option_statement(option)

        return self.query

    async def append_filter_statement(self, option) -> None:

        filter_options = []

        for filter_option in option:

            if filter_option.get('filter_type') == 'in':
                field = filter_option.get('field')
                values = format_values(filter_option.get('values'))

                filter_string = f'{field} IN ({values})'

            else:
                field = filter_option.get('field'),
                filter_type = filter_option.get('filter_type'),
                value = format_value(filter_option.get('value'))

                filter_string = f'{field} {filter_type} {value}'

            filter_options.append(filter_string)

        filter_options=' AND '.join(filter_options)

        self.query = f'{self.query} WHERE {filter_options}'

    async def append_sort_statement(self, option) -> None:

        field=option.get('field')
        sort_type=option.get('order', 'DESC')
        sort_option = f'ORDER BY {field} {sort_type}'

        self.query = f'{self.query} {sort_option}'

    async def append_limit_statement(self, option) -> None:
        limit = option.get('limit')

        self.query = f'{self.query} LIMIT {limit}'

    async def append_if_not_exists(self) -> str:
        self.query = f'{self.query} IF NOT EXISTS'

    def append_time_statement(self, option) -> str:
        if option.get('ttl_seconds') and option.get('with_timestamp'):
            ttl_seconds = option.get('ttl_seconds')
            timestamp = option.get('timestamp')

            self.query = f'{self.query} USING TTL {ttl_seconds} AND TIMESTAMP {timestamp}'

        elif option.get('ttl_seconds'):
            ttl_seconds = option.get('ttl_seconds')

            self.query = f'{self.query} USING TTL {ttl_seconds}'

        elif option.get('using_timestamp'):
            timestamp = option.get('timestamp')

            self.query = f'{self.query} USING TIMESTAMP {timestamp}'

        else:
            timestamp = option.get('timestamp')

            self.query = f'{self.query} TIMESTAMP {timestamp}'
