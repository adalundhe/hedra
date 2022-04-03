from .utils import (
    format_values,
    format_value
)


class OptionsStatment:

    def __init__(self, table_name=None, options=None):
        self.table_name = table_name
        self.options = options

        if options is None:
            options = {}

        self._options_types = {
            'filter': self._append_filter_statement,
            'join': self._append_join_statement,
            'sort': self._append_sort_statement,
            'limit': self._append_limit_statement
        }

        self.join_types = {
            'first': 'LEFT JOIN',
            'second': 'RIGHT JOIN',
            'shared': 'INNER JOIN',
            'both': 'FULL JOIN'
        }

    def apply_options(self, query):
        options_strings = []
        for option in self.options:
            option_type = option.get('type')
            options_string = self._options_types.get(option_type)(
                option.get('values')
            )

            options_strings.append(options_string)

        return '{query} {options}'.format(
            query=query,
            options=' '.join(options_strings)
        )

    def _append_filter_statement(self, option):

        filter_options = []

        for filter_field in option:

            if filter_field.get('type') == 'in':

                filter_string = '{field} IN ({value})'.format(
                    field=filter_field.get('field'),
                    value=','.join([
                        format_value(value) for value in filter_field.get('value')
                    ])
                ) 

            elif filter_field.get('type') == 'ANY':

                filter_string = '{value} = ANY ("{field}")'.format(
                    field=filter_field.get('field'),
                    value=format_value(filter_field.get('value'))
                )

            else:

                filter_string ='{field} {filter_type} {value}'.format(
                    field=filter_field.get('field'),
                    filter_type=filter_field.get('type'),
                    value=format_value(filter_field.get('value'))
                ) 

            filter_options.append(filter_string)

        return 'WHERE {filter_options}'.format(
            filter_options=' AND '.join(filter_options)
        )

    def _append_join_statement(self, option):

        join_statements = []
        
        for join_option in option.get('fields'):
            join_statements.append(
                "{first_table}.{join_field} = {second_table}.{alt_join_field}".format(
                    first_table=self.table_name,
                    join_field=join_option.get('field'),
                    second_table=option.get('table'),
                    alt_join_field=join_option.get(
                        'alt_field', 
                        join_option.get('field')
                    )
                )
            )

        return "{join_type} {table} ON {join_statements}".format(
            join_type=self.join_types.get(
                option.get('matching'),
                'INNER JOIN'
            ),
            table=option.get('table'),
            join_statements=" AND ".join(join_statements)
        )

    def _append_sort_statement(self, option):

        sort_strings = []

        for sort_field in option:
            sort_strings.append(
                '{table_name}.{field_name} {sort_order}'.format(
                    table_name=self.table_name,
                    field_name=sort_field.get('field'),
                    sort_order=sort_field.get('order', 'DESC')
                )
            )


        return 'ORDER BY {sort_option}'.format(
            sort_option=', '.join(sort_strings)
        )

    def _append_limit_statement(self, option):
        return 'LIMIT {count}'.format(
            count=option.get('count')
        )
    

    